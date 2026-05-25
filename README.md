# End-to-End Batch Data Pipeline (W1)

This project implements a complete, end-to-end batch data pipeline designed to scrape books and quotes data, validate records, partition data into Parquet files, store it in an S3-compatible object store (MinIO), load clean data into PostgreSQL, and orchestrate all stages via Apache Airflow, producing a highly stylized HTML summary report.

To accommodate different environments, this project is built with a **dual execution mode**:
1. **Production Mode (Docker Compose)**: Spins up PostgreSQL, MinIO, and Airflow. Recommended for your office laptop.
2. **Local Simulation Mode (Python/SQLite/Local Files)**: A lightweight, offline simulator that replaces PostgreSQL with SQLite and MinIO with a structured local folder. This runs out-of-the-box on systems without Docker.

---

## Project Structure

```text
w1-batch-pipeline/
├── dags/
│   └── weekly_batch_pipeline.py  # Airflow DAG defining the 5 sequential tasks
├── src/
│   ├── __init__.py
│   ├── config.py                 # Core configurations and path resolutions
│   ├── scraper.py                # Beautiful Soup scrapers for books and quotes
│   ├── validator.py              # Null, type, and field-length validations
│   ├── s3_uploader.py            # Parquet conversion and S3/MinIO upload logic
│   ├── db_loader.py              # Ingests Parquet into PostgreSQL/SQLite (idempotent)
│   ├── transformer.py            # Executes SQL to build analytical views
│   └── reporter.py               # Generates premium HTML summary reports
├── data/                         # Local storage for SQLite, logs, and outputs (auto-created)
│   ├── raw/                      # Raw JSON scrapes
│   ├── clean/                    # Intermediate parquets
│   ├── quarantine/               # Reject CSV logs containing validation failure reasons
│   ├── s3_bucket/                # Local S3 simulation folder (partitioned by scrape_date)
│   └── reports/                  # HTML execution reports
├── docker-compose.yml            # Main Docker orchestrator configuration
├── run_pipeline_local.py         # Sequential offline pipeline runner
├── requirements.txt              # Project Python packages list
└── README.md                     # Documentation
```

---

## Installation & Setup

Ensure you have Python 3.10+ installed.

1. **Install Python dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

---

## 1. Local Simulation Mode (Running Offline)

To run the pipeline sequentially on your local machine using SQLite and a local directory S3 mock:

1. **Run the local executor**:
   ```bash
   python run_pipeline_local.py
   ```
2. **Verify the outcomes**:
   - Check the terminal logs to see all stages complete.
   - Database tables, views, and logs are created inside `data/local_pipeline.db`.
   - Raw Parquet files partitioned by date are available in `data/s3_bucket/books/scrape_date=YYYY-MM-DD/books.parquet`.
   - Quarantine files mapping reject reasons are located in `data/quarantine/rejected_books_YYYY-MM-DD.csv`.
   - **View the HTML Summary Report**: Open `data/reports/summary_YYYY-MM-DD.html` in your web browser.

*Note: Running `run_pipeline_local.py` multiple times on the same date is fully idempotent, cleaning up existing records for that date before inserting new ones.*

---

## 2. Production Mode (Docker Compose)

To spin up the entire data platform stack (PostgreSQL, MinIO, Airflow) on your office laptop:

1. **Start the containers**:
   ```bash
   docker compose up -d
   ```
2. **Wait for services to start**:
   - The PostgreSQL database will initialize and create `pipeline_db`.
   - The MinIO container will spin up.
   - The Airflow init container will create the default database schema and a default user (`admin` / `admin`).
3. **Access the Airflow UI**:
   - Open `http://localhost:8080` in your web browser.
   - Log in with username `admin` and password `admin`.
4. **Trigger the DAG**:
   - You will see the DAG named `weekly_batch_pipeline` inside the Airflow UI.
   - Unpause it and trigger the run.
   - The 5 tasks will run sequentially: `scrape` -> `validate` -> `upload_to_s3` -> `load_pg` -> `transform`.
5. **Verify data inside the Docker containers**:
   - **MinIO**: Open `http://localhost:9001` (console) and log in with `minioadmin` / `minioadmin`. Navigate to the `raw-data-bucket` bucket to view the partitioned Parquet files.
   - **PostgreSQL**: Connect to PostgreSQL at `localhost:5432` (database `pipeline_db`, user `postgres`, password `postgres`) to query `raw_books`, `raw_quotes`, `view_category_metrics`, and `view_author_metrics`.
   - **HTML Report**: Check the mapped `data/reports/` folder in the project root to find the HTML report.
