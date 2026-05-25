import logging
from sqlalchemy import text
from src.db_loader import get_db_engine, log_pipeline_stage

logger = logging.getLogger(__name__)

def run_transformations(dag_run_id=None):
    """
    Executes SQL scripts to create clean, analytical views.
    Views are designed to be database-agnostic (working on both SQLite and PostgreSQL).
    """
    logger.info("Starting database transformations to build analytical views")
    engine = get_db_engine()
    
    # Drop existing views in reverse dependency order first (so views depending on others are dropped first)
    drop_queries = [
        "DROP VIEW IF EXISTS view_category_metrics",
        "DROP VIEW IF EXISTS view_author_metrics",
        "DROP VIEW IF EXISTS view_books_cleaned",
        "DROP VIEW IF EXISTS view_quotes_cleaned"
    ]
    
    transform_queries = {
        "view_books_cleaned": """
            CREATE VIEW view_books_cleaned AS
            WITH ranked_books AS (
                SELECT 
                    id, title, price, rating, category, scrape_date, inserted_at,
                    ROW_NUMBER() OVER (PARTITION BY title, category ORDER BY inserted_at DESC) as rn
                FROM raw_books
            )
            SELECT id, title, price, rating, category, scrape_date, inserted_at
            FROM ranked_books
            WHERE rn = 1;
        """,
        "view_quotes_cleaned": """
            CREATE VIEW view_quotes_cleaned AS
            WITH ranked_quotes AS (
                SELECT 
                    id, quote, author, tags, scrape_date, inserted_at,
                    ROW_NUMBER() OVER (PARTITION BY quote, author ORDER BY inserted_at DESC) as rn
                FROM raw_quotes
            )
            SELECT id, quote, author, tags, scrape_date, inserted_at
            FROM ranked_quotes
            WHERE rn = 1;
        """,
        "view_category_metrics": """
            CREATE VIEW view_category_metrics AS
            SELECT 
                category,
                COUNT(*) as total_books,
                ROUND(AVG(price), 2) as average_price,
                MIN(price) as min_price,
                MAX(price) as max_price,
                ROUND(AVG(rating), 1) as average_rating
            FROM view_books_cleaned
            GROUP BY category;
        """,
        "view_author_metrics": """
            CREATE VIEW view_author_metrics AS
            SELECT 
                author,
                COUNT(*) as total_quotes,
                MIN(scrape_date) as first_scraped_date,
                MAX(scrape_date) as last_scraped_date
            FROM view_quotes_cleaned
            GROUP BY author;
        """
    }
    
    stages_completed = 0
    try:
        with engine.begin() as conn:
            # 1. Drop views in reverse dependency order
            logger.info("Dropping existing views...")
            for drop_q in drop_queries:
                conn.execute(text(drop_q))
                
            # 2. Create views in forward dependency order
            for view_name, query in transform_queries.items():
                logger.info(f"Creating analytical view: {view_name}")
                conn.execute(text(query))
                stages_completed += 1
                
        log_pipeline_stage(engine, "transform", "Success", stages_completed, dag_run_id=dag_run_id)
        logger.info("Database transformations completed successfully.")
        return True
    except Exception as e:
        logger.error(f"Failed during transformations stage: {e}")
        log_pipeline_stage(engine, "transform", "Failed", stages_completed, str(e), dag_run_id=dag_run_id)
        raise

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    run_transformations()
