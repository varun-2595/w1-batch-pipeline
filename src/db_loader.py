import logging
import os
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine, text

from src.config import DB_CONN_STR, DB_TYPE, SCRAPE_DATE, LOCAL_S3_PATH

logger = logging.getLogger(__name__)

def get_db_engine():
    """Initializes and returns SQLAlchemy engine."""
    return create_engine(DB_CONN_STR)

def init_db(engine):
    """Initializes database schema if tables do not exist."""
    logger.info(f"Initializing database schema on {DB_TYPE}")
    
    # Define serial/autoincrement based on DB Type
    id_type = "SERIAL PRIMARY KEY" if DB_TYPE == "postgres" else "INTEGER PRIMARY KEY AUTOINCREMENT"
    timestamp_default = "CURRENT_TIMESTAMP"
    
    create_books_table = f"""
    CREATE TABLE IF NOT EXISTS raw_books (
        id {id_type},
        title TEXT,
        price NUMERIC(10, 2),
        rating INTEGER,
        category VARCHAR(100),
        scrape_date DATE,
        inserted_at TIMESTAMP DEFAULT {timestamp_default}
    );
    """
    
    create_quotes_table = f"""
    CREATE TABLE IF NOT EXISTS raw_quotes (
        id {id_type},
        quote TEXT,
        author VARCHAR(100),
        tags TEXT,
        scrape_date DATE,
        inserted_at TIMESTAMP DEFAULT {timestamp_default}
    );
    """
    
    create_log_table = f"""
    CREATE TABLE IF NOT EXISTS pipeline_run_log (
        id {id_type},
        dag_run_id VARCHAR(100),
        stage VARCHAR(50),
        status VARCHAR(20),
        record_count INTEGER,
        error_message TEXT,
        timestamp TIMESTAMP DEFAULT {timestamp_default}
    );
    """
    
    with engine.begin() as conn:
        conn.execute(text(create_books_table))
        conn.execute(text(create_quotes_table))
        conn.execute(text(create_log_table))
        
    logger.info("Database tables initialized.")

def log_pipeline_stage(engine, stage, status, record_count=0, error_message=None, dag_run_id=None):
    """Logs the execution status of a pipeline stage to pipeline_run_log table."""
    if dag_run_id is None:
        dag_run_id = f"manual_run_{SCRAPE_DATE}"
        
    insert_log_query = """
    INSERT INTO pipeline_run_log (dag_run_id, stage, status, record_count, error_message)
    VALUES (:dag_run_id, :stage, :status, :record_count, :error_message)
    """
    
    try:
        with engine.begin() as conn:
            conn.execute(
                text(insert_log_query),
                {
                    "dag_run_id": dag_run_id,
                    "stage": stage,
                    "status": status,
                    "record_count": record_count,
                    "error_message": error_message
                }
            )
        logger.info(f"Logged stage '{stage}' with status '{status}' (records: {record_count})")
    except Exception as e:
        logger.error(f"Failed to write log to pipeline_run_log: {e}")

def load_parquet_to_db(engine, parquet_path_or_buffer, table_name, scrape_date):
    """
    Loads data from a Parquet path or buffer into raw_books or raw_quotes.
    Deletes existing records for that scrape_date first to ensure idempotency.
    """
    logger.info(f"Loading Parquet data into {table_name} for date {scrape_date}")
    
    # Read Parquet
    df = pd.read_parquet(parquet_path_or_buffer)
    
    if df.empty:
        logger.info(f"No records found in Parquet to load into {table_name}.")
        return 0

    # Ensure clean data mapping
    df["scrape_date"] = datetime.strptime(scrape_date, "%Y-%m-%d").date()
    df["inserted_at"] = datetime.now()
    
    # For quotes, tags is a list. Serialize list to comma-separated string for DB storage
    if "tags" in df.columns:
        df["tags"] = df["tags"].apply(lambda t: ",".join(t) if isinstance(t, list) or isinstance(t, val.__class__ if 'val' in locals() else list) else str(t))

    with engine.begin() as conn:
        # Idempotency step: Clean up existing data for this date
        delete_query = f"DELETE FROM {table_name} WHERE scrape_date = :scrape_date"
        conn.execute(text(delete_query), {"scrape_date": scrape_date})
        logger.info(f"Deleted existing records in {table_name} for date {scrape_date} to prevent duplicates.")

    # Write records using pandas to_sql (SQLAlchemy manages connections)
    # to_sql automatically creates columns if not exists, but we predefined table schema to enforce types
    # Postgres needs table lowercase and matches column types
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False
    )
    
    logger.info(f"Successfully loaded {len(df)} records into {table_name}.")
    return len(df)

def load_clean_data_to_db(books_path_or_buffer, quotes_path_or_buffer, dag_run_id=None):
    """
    Main function to run the database ingestion.
    """
    engine = get_db_engine()
    init_db(engine)
    
    books_count = 0
    quotes_count = 0
    
    # 1. Load Books
    if books_path_or_buffer:
        try:
            books_count = load_parquet_to_db(engine, books_path_or_buffer, "raw_books", SCRAPE_DATE)
            log_pipeline_stage(engine, "load_books", "Success", books_count, dag_run_id=dag_run_id)
        except Exception as e:
            logger.error(f"Failed to load books into database: {e}")
            log_pipeline_stage(engine, "load_books", "Failed", 0, str(e), dag_run_id=dag_run_id)
            raise
            
    # 2. Load Quotes
    if quotes_path_or_buffer:
        try:
            quotes_count = load_parquet_to_db(engine, quotes_path_or_buffer, "raw_quotes", SCRAPE_DATE)
            log_pipeline_stage(engine, "load_quotes", "Success", quotes_count, dag_run_id=dag_run_id)
        except Exception as e:
            logger.error(f"Failed to load quotes into database: {e}")
            log_pipeline_stage(engine, "load_quotes", "Failed", 0, str(e), dag_run_id=dag_run_id)
            raise
            
    return books_count, quotes_count

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Test initialization
    engine = get_db_engine()
    init_db(engine)
