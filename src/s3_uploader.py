import io
import logging
import os
import pandas as pd
import boto3
from botocore.client import Config

from src.config import (
    LOCAL_S3_PATH, S3_ENDPOINT_URL, S3_ACCESS_KEY,
    S3_SECRET_KEY, S3_BUCKET, SCRAPE_DATE
)

logger = logging.getLogger(__name__)

def get_s3_client():
    """Initializes and returns a boto3 S3 client configured for MinIO."""
    if not S3_ENDPOINT_URL:
        return None
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def ensure_bucket_exists(s3_client, bucket_name):
    """Checks if the bucket exists, and creates it if not."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        logger.info(f"S3 Bucket '{bucket_name}' already exists.")
    except Exception:
        logger.info(f"S3 Bucket '{bucket_name}' does not exist. Creating it...")
        try:
            s3_client.create_bucket(Bucket=bucket_name)
            logger.info(f"S3 Bucket '{bucket_name}' created successfully.")
        except Exception as e:
            logger.error(f"Failed to create S3 Bucket '{bucket_name}': {e}")
            raise

def upload_dataframe_to_s3(df, s3_path_key):
    """
    Saves a DataFrame as Parquet in memory and uploads it to MinIO/S3,
    or writes it to a local folder in local simulation mode.
    """
    s3_client = get_s3_client()
    
    # 1. Local Simulation Mode
    if s3_client is None:
        local_path = os.path.join(LOCAL_S3_PATH, s3_path_key.replace("/", os.sep))
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        df.to_parquet(local_path, index=False)
        logger.info(f"[Local Simulation] Saved parquet to {local_path}")
        return local_path
        
    # 2. Production S3/MinIO Mode
    else:
        ensure_bucket_exists(s3_client, S3_BUCKET)
        
        # Save Parquet to bytes buffer
        parquet_buffer = io.BytesIO()
        df.to_parquet(parquet_buffer, index=False)
        parquet_buffer.seek(0)
        
        try:
            s3_client.put_object(
                Bucket=S3_BUCKET,
                Key=s3_path_key,
                Body=parquet_buffer.getvalue()
            )
            logger.info(f"[Production S3] Uploaded parquet to s3://{S3_BUCKET}/{s3_path_key}")
            return f"s3://{S3_BUCKET}/{s3_path_key}"
        except Exception as e:
            logger.error(f"Failed to upload parquet to s3://{S3_BUCKET}/{s3_path_key}: {e}")
            raise

def upload_clean_data(clean_books, clean_quotes):
    """
    Orchestrates partitioning and uploading of clean data to S3.
    """
    logger.info("Starting upload of clean data to S3 storage")
    
    # 1. Upload Books
    books_uploaded_path = None
    if clean_books:
        df_books = pd.DataFrame(clean_books)
        # Parquet structure partitioned by scrape_date=YYYY-MM-DD
        # Note: In standard Spark/Hive partitions, the path contains `key=value`
        books_key = f"books/scrape_date={SCRAPE_DATE}/books.parquet"
        books_uploaded_path = upload_dataframe_to_s3(df_books, books_key)
    else:
        logger.warning("No clean books to upload.")

    # 2. Upload Quotes
    quotes_uploaded_path = None
    if clean_quotes:
        df_quotes = pd.DataFrame(clean_quotes)
        # Quotes has tags as list, pyarrow handles it natively as text array list!
        quotes_key = f"quotes/scrape_date={SCRAPE_DATE}/quotes.parquet"
        # Since tags are lists, let's make sure they are arrays in pandas
        quotes_uploaded_path = upload_dataframe_to_s3(df_quotes, quotes_key)
    else:
        logger.warning("No clean quotes to upload.")
        
    return books_uploaded_path, quotes_uploaded_path

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Test upload with sample data
    test_books = [{"title": "Test Book", "price": 10.99, "rating": 5, "category": "Test"}]
    test_quotes = [{"quote": "Test Quote", "author": "Test Author", "tags": ["test", "tag"]}]
    upload_clean_data(test_books, test_quotes)
