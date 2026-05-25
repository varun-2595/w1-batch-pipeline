import json
import logging
import re
import time
from urllib.parse import urljoin
import requests
from bs4 import BeautifulSoup

from src.config import BOOKS_URL, QUOTES_URL, RAW_BOOKS_PATH, RAW_QUOTES_PATH

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

RATING_MAP = {
    "one": 1,
    "two": 2,
    "three": 3,
    "four": 4,
    "five": 5
}

def clean_price(price_str):
    """Extract numeric value from price string like £51.77 or Â£51.77"""
    if not price_str:
        return None
    # Extract any digits and decimal point
    match = re.search(r"(\d+\.\d+|\d+)", price_str)
    if match:
        return float(match.group(1))
    return None

def scrape_books(max_categories=10):
    """
    Crawls books.toscrape.com categories.
    For the first max_categories, crawls all books.
    """
    logger.info(f"Starting Books scraping from {BOOKS_URL}")
    books = []
    
    try:
        response = requests.get(BOOKS_URL, timeout=15)
        response.raise_for_status()
    except Exception as e:
        logger.error(f"Failed to fetch Books homepage: {e}")
        return []

    soup = BeautifulSoup(response.content, "html.parser")
    
    # Find category links from sidebar
    # The sidebar lists categories. The first one is "Books" (which contains everything), followed by subcategories.
    sidebar = soup.select_one(".side_categories ul.nav-list ul")
    if not sidebar:
        logger.error("Could not find side categories menu.")
        return []
        
    category_links = sidebar.select("a")
    logger.info(f"Found {len(category_links)} categories. Scraping first {max_categories}.")
    
    for cat_idx, cat_link in enumerate(category_links[:max_categories]):
        category_name = cat_link.text.strip()
        category_href = cat_link["href"]
        category_url = urljoin(BOOKS_URL, category_href)
        
        logger.info(f"Scraping Category ({cat_idx+1}/{max_categories}): {category_name}")
        
        # Scrape category pages (handling pagination within the category)
        current_url = category_url
        while current_url:
            try:
                cat_resp = requests.get(current_url, timeout=15)
                cat_resp.raise_for_status()
            except Exception as e:
                logger.error(f"Failed to fetch category page {current_url}: {e}")
                break
                
            cat_soup = BeautifulSoup(cat_resp.content, "html.parser")
            
            # Find books on page
            product_pods = cat_soup.select("article.product_pod")
            for pod in product_pods:
                # Title is in <h3><a title="...">
                title_el = pod.select_one("h3 a")
                title = title_el["title"] if title_el and title_el.has_attr("title") else None
                if not title and title_el:
                    title = title_el.text.strip()
                
                # Price is in <p class="price_color">
                price_el = pod.select_one(".price_color")
                price_text = price_el.text.strip() if price_el else None
                price = clean_price(price_text)
                
                # Rating is class in <p class="star-rating Three">
                rating = None
                rating_el = pod.select_one(".star-rating")
                if rating_el:
                    classes = rating_el["class"]
                    for c in classes:
                        c_lower = c.lower()
                        if c_lower in RATING_MAP:
                            rating = RATING_MAP[c_lower]
                            break
                            
                books.append({
                    "title": title,
                    "price": price,
                    "rating": rating,
                    "category": category_name
                })
            
            # Check for "next" page in pagination
            next_btn = cat_soup.select_one("li.next a")
            if next_btn:
                next_href = next_btn["href"]
                current_url = urljoin(current_url, next_href)
            else:
                current_url = None
                
            # Nice scrapers sleep a bit
            time.sleep(0.2)
            
    logger.info(f"Completed Books scraping. Extracted {len(books)} records.")
    return books

def scrape_quotes(max_pages=5):
    """
    Crawls quotes.toscrape.com up to max_pages.
    """
    logger.info(f"Starting Quotes scraping from {QUOTES_URL}")
    quotes = []
    
    current_url = QUOTES_URL
    page_count = 0
    
    while current_url and page_count < max_pages:
        page_count += 1
        logger.info(f"Scraping Quotes Page {page_count}/{max_pages}: {current_url}")
        
        try:
            response = requests.get(current_url, timeout=15)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"Failed to fetch quotes page {current_url}: {e}")
            break
            
        soup = BeautifulSoup(response.content, "html.parser")
        quote_elements = soup.select("div.quote")
        
        for q_el in quote_elements:
            # Quote text
            text_el = q_el.select_one("span.text")
            # Stripping the curly quotes
            quote_text = text_el.text.strip().strip("“”") if text_el else None
            
            # Author
            author_el = q_el.select_one("small.author")
            author = author_el.text.strip() if author_el else None
            
            # Tags
            tag_elements = q_el.select("div.tags a.tag")
            tags = [tag.text.strip() for tag in tag_elements]
            
            quotes.append({
                "quote": quote_text,
                "author": author,
                "tags": tags
            })
            
        # Pagination
        next_btn = soup.select_one("li.next a")
        if next_btn:
            next_href = next_btn["href"]
            current_url = urljoin(QUOTES_URL, next_href)
        else:
            current_url = None
            
        time.sleep(0.2)
        
    logger.info(f"Completed Quotes scraping. Extracted {len(quotes)} records.")
    return quotes

def run_scraper(max_categories=10, max_pages=5):
    """
    Orchestrates the scraping process and writes results to JSON files.
    """
    books_data = scrape_books(max_categories=max_categories)
    quotes_data = scrape_quotes(max_pages=max_pages)
    
    # Save raw books to JSON
    with open(RAW_BOOKS_PATH, "w", encoding="utf-8") as f:
        json.dump(books_data, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved raw books data to {RAW_BOOKS_PATH}")
    
    # Save raw quotes to JSON
    with open(RAW_QUOTES_PATH, "w", encoding="utf-8") as f:
        json.dump(quotes_data, f, ensure_ascii=False, indent=2)
    logger.info(f"Saved raw quotes data to {RAW_QUOTES_PATH}")
    
    return len(books_data), len(quotes_data)

if __name__ == "__main__":
    run_scraper(max_categories=5, max_pages=5)
