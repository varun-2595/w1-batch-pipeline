import logging
import os
import sys
import time
from datetime import datetime

# Ensure data directory exists for logs
os.path.abspath(__file__)
data_dir_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data")
os.makedirs(data_dir_path, exist_ok=True)

# Set up logging to both file and console
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(os.path.join(data_dir_path, "local_pipeline.log"), encoding="utf-8")
    ]
)
logger = logging.getLogger("LocalPipelineRunner")

# Ensure project root is in sys.path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from src.config import (
    SCRAPE_DATE, DATA_DIR, RAW_BOOKS_PATH, RAW_QUOTES_PATH,
    QUARANTINE_BOOKS_PATH, QUARANTINE_QUOTES_PATH, LOCAL_S3_PATH,
    DB_CONN_STR
)
from src.scraper import run_scraper
from src.validator import run_validation, validate_books, validate_quotes
from src.s3_uploader import upload_clean_data
from src.db_loader import get_db_engine, init_db, load_clean_data_to_db, log_pipeline_stage
from src.transformer import run_transformations
from src.reporter import generate_html_report

def print_banner(task_name):
    border = "=" * 60
    logger.info(f"\n{border}\n[TASK RUN] Starting task: {task_name}\n{border}")

def print_success(task_name, duration, results=None):
    border = "-" * 60
    logger.info(f"[TASK SUCCESS] Completed: {task_name} in {duration:.2f}s")
    if results:
        logger.info(f"Results: {results}")
    logger.info(f"{border}\n")

def run_local_pipeline():
    logger.info("=" * 80)
    logger.info(f"STARTING LOCAL PIPELINE SIMULATION FOR DATE: {SCRAPE_DATE}")
    logger.info(f"Data Directory: {DATA_DIR}")
    logger.info(f"Database Type: SQLite ({DB_CONN_STR})")
    logger.info("=" * 80 + "\n")
    
    start_time = time.time()
    dag_run_id = f"local_sim_{datetime.now().strftime('%Y%m%dT%H%M%S')}"
    
    # 0. Initialize DB schema
    engine = get_db_engine()
    init_db(engine)
    log_pipeline_stage(engine, "dag_start", "Success", 0, dag_run_id=dag_run_id)

    try:
        # Task 1: Scrape
        t_start = time.time()
        print_banner("1. scrape")
        # Scrape 5 categories of books, 3 pages of quotes (to keep run fast but realistic)
        books_scraped, quotes_scraped = run_scraper(max_categories=5, max_pages=3)
        duration = time.time() - t_start
        print_success(
            "1. scrape", 
            duration, 
            {"books_scraped": books_scraped, "quotes_scraped": quotes_scraped}
        )
        
        # Task 2: Validate
        t_start = time.time()
        print_banner("2. validate")
        clean_books_cnt, clean_quotes_cnt = run_validation()
        duration = time.time() - t_start
        print_success(
            "2. validate", 
            duration, 
            {"clean_books": clean_books_cnt, "clean_quotes": clean_quotes_cnt}
        )
        
        # Task 3: Upload S3
        t_start = time.time()
        print_banner("3. upload_to_s3")
        # Reload validation to get clean records list
        clean_books_list = validate_books(RAW_BOOKS_PATH, QUARANTINE_BOOKS_PATH)
        clean_quotes_list = validate_quotes(RAW_QUOTES_PATH, QUARANTINE_QUOTES_PATH)
        
        books_s3, quotes_s3 = upload_clean_data(clean_books_list, clean_quotes_list)
        duration = time.time() - t_start
        print_success(
            "3. upload_to_s3", 
            duration, 
            {"books_parquet_path": books_s3, "quotes_parquet_path": quotes_s3}
        )
        
        # Task 4: Load PG (SQLite fallback)
        t_start = time.time()
        print_banner("4. load_pg (SQLite)")
        # For SQLite fallback, the uploader paths are local file paths
        books_loaded, quotes_loaded = load_clean_data_to_db(books_s3, quotes_s3, dag_run_id=dag_run_id)
        duration = time.time() - t_start
        print_success(
            "4. load_pg (SQLite)", 
            duration, 
            {"books_loaded": books_loaded, "quotes_loaded": quotes_loaded}
        )
        
        # Task 5: Transform & Report
        t_start = time.time()
        print_banner("5. transform (Views & Report)")
        run_transformations(dag_run_id=dag_run_id)
        report_path = generate_html_report(dag_run_id=dag_run_id)
        duration = time.time() - t_start
        print_success(
            "5. transform (Views & Report)", 
            duration, 
            {"html_report_path": report_path}
        )
        
        total_duration = time.time() - start_time
        log_pipeline_stage(engine, "dag_end", "Success", 0, dag_run_id=dag_run_id)
        logger.info("=" * 80)
        logger.info(f"LOCAL PIPELINE RUN COMPLETED SUCCESSFULLY in {total_duration:.2f}s")
        logger.info(f"You can view the HTML report here: {report_path}")
        logger.info("=" * 80)
        
    except Exception as e:
        logger.exception("Pipeline execution failed!")
        log_pipeline_stage(engine, "dag_end", "Failed", 0, str(e), dag_run_id=dag_run_id)
        sys.exit(1)

if __name__ == "__main__":
    run_local_pipeline()
