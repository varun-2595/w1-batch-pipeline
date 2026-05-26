import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

# Default arguments for the Airflow DAG
default_args = {
    "owner": "data_engineer",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

def scrape_stage(**kwargs):
    """Stage 1: Scrape raw books and quotes data."""
    ds = datetime.now().strftime("%Y-%m-%d")
    os.environ["SCRAPE_DATE"] = ds
    
    from src.scraper import run_scraper
    # Scrape first 5 categories of books and first 5 pages of quotes
    books_cnt, quotes_cnt = run_scraper(max_categories=5, max_pages=5)
    return {"raw_books_count": books_cnt, "raw_quotes_count": quotes_cnt}

def validate_stage(**kwargs):
    """Stage 2: Run validation and filter rejects."""
    ds = datetime.now().strftime("%Y-%m-%d")
    os.environ["SCRAPE_DATE"] = ds
    
    from src.validator import run_validation
    clean_books, clean_quotes = run_validation()
    return {"clean_books_count": clean_books, "clean_quotes_count": clean_quotes}

def upload_s3_stage(**kwargs):
    """Stage 3: Format clean data to Parquet and upload to MinIO/S3."""
    ds = datetime.now().strftime("%Y-%m-%d")
    os.environ["SCRAPE_DATE"] = ds
    
    from src.validator import validate_books, validate_quotes
    from src.config import RAW_BOOKS_PATH, RAW_QUOTES_PATH, QUARANTINE_BOOKS_PATH, QUARANTINE_QUOTES_PATH
    
    # Reload validated clean lists to upload
    clean_b = validate_books(RAW_BOOKS_PATH, QUARANTINE_BOOKS_PATH)
    clean_q = validate_quotes(RAW_QUOTES_PATH, QUARANTINE_QUOTES_PATH)
    
    from src.s3_uploader import upload_clean_data
    books_s3_path, quotes_s3_path = upload_clean_data(clean_b, clean_q)
    return {"books_s3_path": books_s3_path, "quotes_s3_path": quotes_s3_path}

def load_pg_stage(**kwargs):
    """Stage 4: Ingest Parquet data from S3 into PostgreSQL raw tables."""
    ds = datetime.now().strftime("%Y-%m-%d")
    run_id = kwargs.get("run_id")
    os.environ["SCRAPE_DATE"] = ds
    
    from src.config import RUNNING_IN_DOCKER, LOCAL_S3_PATH, S3_ENDPOINT_URL
    import io
    import boto3
    from botocore.client import Config
    
    # Connect to S3 and fetch parquet bytes, or fetch local file
    if RUNNING_IN_DOCKER:
        s3_client = boto3.client(
            "s3",
            endpoint_url=S3_ENDPOINT_URL,
            aws_access_key_id=os.environ.get("S3_ACCESS_KEY", "minioadmin"),
            aws_secret_access_key=os.environ.get("S3_SECRET_KEY", "minioadmin"),
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"
        )
        
        books_obj = s3_client.get_object(Bucket="raw-data-bucket", Key=f"books/scrape_date={ds}/books.parquet")
        books_input = io.BytesIO(books_obj["Body"].read())
        
        quotes_obj = s3_client.get_object(Bucket="raw-data-bucket", Key=f"quotes/scrape_date={ds}/quotes.parquet")
        quotes_input = io.BytesIO(quotes_obj["Body"].read())
    else:
        books_input = os.path.join(LOCAL_S3_PATH, "books", f"scrape_date={ds}", "books.parquet")
        quotes_input = os.path.join(LOCAL_S3_PATH, "quotes", f"scrape_date={ds}", "quotes.parquet")
        
    from src.db_loader import load_clean_data_to_db
    books_loaded, quotes_loaded = load_clean_data_to_db(books_input, quotes_input, dag_run_id=run_id)
    return {"books_loaded": books_loaded, "quotes_loaded": quotes_loaded}

def transform_stage(**kwargs):
    """Stage 5: Build analytical views and generate HTML summary report."""
    ds = datetime.now().strftime("%Y-%m-%d")
    run_id = kwargs.get("run_id")
    os.environ["SCRAPE_DATE"] = ds
    
    from src.transformer import run_transformations
    run_transformations(dag_run_id=run_id)
    
    from src.reporter import generate_html_report
    report_path = generate_html_report(dag_run_id=run_id)
    return {"report_path": report_path}

# Define the DAG
with DAG(
    "weekly_batch_pipeline",
    default_args=default_args,
    description="Batch pipeline that scrapes websites, validates, uploads Parquet to S3, loads PostgreSQL, and generates HTML reports",
    schedule_interval="@weekly",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["data_engineering", "books", "quotes"],
) as dag:

    scrape_task = PythonOperator(
        task_id="scrape",
        python_callable=scrape_stage,
    )

    validate_task = PythonOperator(
        task_id="validate",
        python_callable=validate_stage,
    )

    upload_s3_task = PythonOperator(
        task_id="upload_to_s3",
        python_callable=upload_s3_stage,
    )

    load_pg_task = PythonOperator(
        task_id="load_pg",
        python_callable=load_pg_stage,
    )

    transform_task = PythonOperator(
        task_id="transform",
        python_callable=transform_stage,
    )

    # Establish dependency chain
    scrape_task >> validate_task >> upload_s3_task >> load_pg_task >> transform_task
