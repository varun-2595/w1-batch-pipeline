import json
import logging
import os
import pandas as pd
from src.config import (
    RAW_BOOKS_PATH, RAW_QUOTES_PATH,
    QUARANTINE_BOOKS_PATH, QUARANTINE_QUOTES_PATH
)

logger = logging.getLogger(__name__)

def validate_books(raw_books_path, quarantine_books_path):
    """
    Validates books raw data.
    Null checks, price format/range checks, and length checks.
    Quarantines failures to CSV.
    """
    logger.info(f"Starting Books validation for {raw_books_path}")
    if not os.path.exists(raw_books_path):
        logger.error(f"Raw books file not found at {raw_books_path}")
        return []

    with open(raw_books_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    clean_records = []
    rejected_records = []

    for idx, r in enumerate(records):
        failures = []
        
        # 1. Null/Empty checks
        title = r.get("title")
        price = r.get("price")
        rating = r.get("rating")
        category = r.get("category")

        if not title or str(title).strip() == "":
            failures.append("Title is null or empty")
        if price is None:
            failures.append("Price is null or invalid format")
        if rating is None:
            failures.append("Rating is null or invalid")
        if not category or str(category).strip() == "":
            failures.append("Category is null or empty")

        # 2. Value checks (if price exists)
        if price is not None:
            try:
                price_val = float(price)
                if price_val <= 0:
                    failures.append(f"Price is non-positive: {price_val}")
            except (ValueError, TypeError):
                failures.append(f"Price is not a valid decimal: {price}")

        # 3. Field length checks
        if title and len(str(title)) > 255:
            failures.append(f"Title length exceeds 255 chars (length: {len(str(title))})")
        if category and len(str(category)) > 100:
            failures.append(f"Category length exceeds 100 chars (length: {len(str(category))})")

        # Quarantine check
        if failures:
            rejected_records.append({
                "record_index": idx,
                "title": title or "",
                "price": price if price is not None else "",
                "rating": rating if rating is not None else "",
                "category": category or "",
                "rejection_reasons": "; ".join(failures)
            })
        else:
            clean_records.append({
                "title": str(title).strip(),
                "price": float(price),
                "rating": int(rating),
                "category": str(category).strip()
            })

    # Save quarantined records
    if rejected_records:
        df_rej = pd.DataFrame(rejected_records)
        df_rej.to_csv(quarantine_books_path, index=False, encoding="utf-8")
        logger.warning(f"Quarantined {len(rejected_records)} books to {quarantine_books_path}")
    else:
        # Create empty quarantine CSV with headers if no rejects
        df_rej = pd.DataFrame(columns=["record_index", "title", "price", "rating", "category", "rejection_reasons"])
        df_rej.to_csv(quarantine_books_path, index=False, encoding="utf-8")

    logger.info(f"Books validation complete. Clean: {len(clean_records)}, Rejected: {len(rejected_records)}")
    return clean_records

def validate_quotes(raw_quotes_path, quarantine_quotes_path):
    """
    Validates quotes raw data.
    Null checks and field length constraints.
    Quarantines failures to CSV.
    """
    logger.info(f"Starting Quotes validation for {raw_quotes_path}")
    if not os.path.exists(raw_quotes_path):
        logger.error(f"Raw quotes file not found at {raw_quotes_path}")
        return []

    with open(raw_quotes_path, "r", encoding="utf-8") as f:
        records = json.load(f)

    clean_records = []
    rejected_records = []

    for idx, r in enumerate(records):
        failures = []
        
        # 1. Null/Empty checks
        quote = r.get("quote")
        author = r.get("author")
        tags = r.get("tags")  # tags is a list

        if not quote or str(quote).strip() == "":
            failures.append("Quote text is null or empty")
        if not author or str(author).strip() == "":
            failures.append("Author is null or empty")

        # 2. Length checks
        if quote and len(str(quote)) > 1000:
            failures.append(f"Quote length exceeds 1000 chars (length: {len(str(quote))})")
        if author and len(str(author)) > 100:
            failures.append(f"Author length exceeds 100 chars (length: {len(str(author))})")

        # Quarantine check
        if failures:
            rejected_records.append({
                "record_index": idx,
                "quote": quote or "",
                "author": author or "",
                "tags": ",".join(tags) if tags else "",
                "rejection_reasons": "; ".join(failures)
            })
        else:
            clean_records.append({
                "quote": str(quote).strip(),
                "author": str(author).strip(),
                "tags": tags if isinstance(tags, list) else []
            })

    # Save quarantined records
    if rejected_records:
        df_rej = pd.DataFrame(rejected_records)
        df_rej.to_csv(quarantine_quotes_path, index=False, encoding="utf-8")
        logger.warning(f"Quarantined {len(rejected_records)} quotes to {quarantine_quotes_path}")
    else:
        # Create empty quarantine CSV with headers if no rejects
        df_rej = pd.DataFrame(columns=["record_index", "quote", "author", "tags", "rejection_reasons"])
        df_rej.to_csv(quarantine_quotes_path, index=False, encoding="utf-8")

    logger.info(f"Quotes validation complete. Clean: {len(clean_records)}, Rejected: {len(rejected_records)}")
    return clean_records

def run_validation():
    """
    Runs books and quotes validations.
    """
    clean_books = validate_books(RAW_BOOKS_PATH, QUARANTINE_BOOKS_PATH)
    clean_quotes = validate_quotes(RAW_QUOTES_PATH, QUARANTINE_QUOTES_PATH)
    return len(clean_books), len(clean_quotes)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_validation()
