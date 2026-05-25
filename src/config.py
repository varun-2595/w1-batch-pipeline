import os
from datetime import datetime

# Root directory of the project
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Core switches
# We default to sqlite if we are not running inside Airflow docker environment
RUNNING_IN_DOCKER = os.environ.get("RUNNING_IN_DOCKER", "False").lower() == "true"

# Date for partitioning (Default to today YYYY-MM-DD)
# Can be overridden by env variable (e.g., in Airflow context using ds)
SCRAPE_DATE = os.environ.get("SCRAPE_DATE", datetime.now().strftime("%Y-%m-%d"))

# Database Configurations
if RUNNING_IN_DOCKER:
    # Production values when running inside Docker Compose
    DB_CONN_STR = os.environ.get(
        "DB_CONN_STR", 
        "postgresql://postgres:postgres@postgres:5432/pipeline_db"
    )
    DB_TYPE = "postgres"
    
    # MinIO / S3 Configurations
    S3_ENDPOINT_URL = os.environ.get("S3_ENDPOINT_URL", "http://minio:9000")
    S3_ACCESS_KEY = os.environ.get("S3_ACCESS_KEY", "minioadmin")
    S3_SECRET_KEY = os.environ.get("S3_SECRET_KEY", "minioadmin")
    S3_BUCKET = os.environ.get("S3_BUCKET", "raw-data-bucket")
    LOCAL_S3_PATH = None
else:
    # Local simulation fallback values
    DB_CONN_STR = os.environ.get(
        "DB_CONN_STR", 
        f"sqlite:///{os.path.join(DATA_DIR, 'local_pipeline.db')}"
    )
    DB_TYPE = "sqlite"
    
    # Simulating S3 as a local partitioned directory structure
    S3_ENDPOINT_URL = None
    S3_ACCESS_KEY = None
    S3_SECRET_KEY = None
    S3_BUCKET = "raw-data-bucket"
    LOCAL_S3_PATH = os.path.join(DATA_DIR, "s3_bucket")

# Scraping Settings
BOOKS_URL = "https://books.toscrape.com/"
QUOTES_URL = "https://quotes.toscrape.com/"

# Raw temporary files locations
RAW_BOOKS_PATH = os.path.join(DATA_DIR, "raw", f"books_{SCRAPE_DATE}.json")
RAW_QUOTES_PATH = os.path.join(DATA_DIR, "raw", f"quotes_{SCRAPE_DATE}.json")

# Quarantine / Rejected locations
QUARANTINE_BOOKS_PATH = os.path.join(DATA_DIR, "quarantine", f"rejected_books_{SCRAPE_DATE}.csv")
QUARANTINE_QUOTES_PATH = os.path.join(DATA_DIR, "quarantine", f"rejected_quotes_{SCRAPE_DATE}.csv")

# Clean/Validated local files (before uploading to S3)
CLEAN_BOOKS_PATH = os.path.join(DATA_DIR, "clean", f"clean_books_{SCRAPE_DATE}.parquet")
CLEAN_QUOTES_PATH = os.path.join(DATA_DIR, "clean", f"clean_quotes_{SCRAPE_DATE}.parquet")

# HTML Report Output Path
REPORT_PATH = os.path.join(DATA_DIR, "reports", f"summary_{SCRAPE_DATE}.html")

# Create local directories for data flow
os.makedirs(os.path.join(DATA_DIR, "raw"), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "clean"), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "quarantine"), exist_ok=True)
os.makedirs(os.path.join(DATA_DIR, "reports"), exist_ok=True)
if LOCAL_S3_PATH:
    os.makedirs(LOCAL_S3_PATH, exist_ok=True)
