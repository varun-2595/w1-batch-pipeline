import logging
import os
import pandas as pd
from sqlalchemy import text
from src.db_loader import get_db_engine, log_pipeline_stage
from src.config import (
    REPORT_PATH, QUARANTINE_BOOKS_PATH, QUARANTINE_QUOTES_PATH,
    SCRAPE_DATE, DB_CONN_STR
)

logger = logging.getLogger(__name__)

def generate_html_report(dag_run_id=None):
    """
    Queries the database and reads quarantine files to generate
    a rich, premium HTML Summary Report of the pipeline run.
    """
    logger.info("Generating HTML summary report")
    engine = get_db_engine()
    
    if dag_run_id is None:
        dag_run_id = f"manual_run_{SCRAPE_DATE}"
        
    try:
        # 1. Fetch metrics from DB
        with engine.connect() as conn:
            # Book counts
            total_raw_books = conn.execute(text("SELECT COUNT(*) FROM raw_books")).scalar() or 0
            total_clean_books = conn.execute(text("SELECT COUNT(*) FROM view_books_cleaned")).scalar() or 0
            
            # Quote counts
            total_raw_quotes = conn.execute(text("SELECT COUNT(*) FROM raw_quotes")).scalar() or 0
            total_clean_quotes = conn.execute(text("SELECT COUNT(*) FROM view_quotes_cleaned")).scalar() or 0
            
            # Category metrics
            cat_df = pd.read_sql(
                "SELECT category, total_books, average_price, average_rating FROM view_category_metrics ORDER BY total_books DESC LIMIT 10",
                conn
            )
            
            # Author metrics
            auth_df = pd.read_sql(
                "SELECT author, total_quotes FROM view_author_metrics ORDER BY total_quotes DESC LIMIT 10",
                conn
            )
            
            # Run logs
            logs_df = pd.read_sql(
                "SELECT stage, status, record_count, timestamp FROM pipeline_run_log ORDER BY timestamp ASC",
                conn
            )
            
        # 2. Fetch quarantine data
        rejected_books_count = 0
        rejected_books_sample = []
        if os.path.exists(QUARANTINE_BOOKS_PATH):
            try:
                rej_books_df = pd.read_csv(QUARANTINE_BOOKS_PATH)
                rejected_books_count = len(rej_books_df)
                rejected_books_sample = rej_books_df.head(5).to_dict(orient="records")
            except Exception as e:
                logger.warning(f"Error reading books quarantine file: {e}")
                
        rejected_quotes_count = 0
        rejected_quotes_sample = []
        if os.path.exists(QUARANTINE_QUOTES_PATH):
            try:
                rej_quotes_df = pd.read_csv(QUARANTINE_QUOTES_PATH)
                rejected_quotes_count = len(rej_quotes_df)
                rejected_quotes_sample = rej_quotes_df.head(5).to_dict(orient="records")
            except Exception as e:
                logger.warning(f"Error reading quotes quarantine file: {e}")

        # Compute aggregates
        total_scraped = total_raw_books + total_raw_quotes + rejected_books_count + rejected_quotes_count
        total_clean = total_clean_books + total_clean_quotes
        total_rejected = rejected_books_count + rejected_quotes_count

        # 3. Generate HTML Content
        html_template = get_html_template(
            scrape_date=SCRAPE_DATE,
            dag_run_id=dag_run_id,
            total_scraped=total_scraped,
            total_clean=total_clean,
            total_rejected=total_rejected,
            raw_books=total_raw_books,
            clean_books=total_clean_books,
            rejected_books=rejected_books_count,
            raw_quotes=total_raw_quotes,
            clean_quotes=total_clean_quotes,
            rejected_quotes=rejected_quotes_count,
            categories=cat_df.to_dict(orient="records"),
            authors=auth_df.to_dict(orient="records"),
            logs=logs_df.to_dict(orient="records"),
            books_rejects=rejected_books_sample,
            quotes_rejects=rejected_quotes_sample
        )
        
        # Make sure reports directory exists
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)
        with open(REPORT_PATH, "w", encoding="utf-8") as f:
            f.write(html_template)
            
        logger.info(f"Summary report generated at: {REPORT_PATH}")
        log_pipeline_stage(engine, "report", "Success", 1, dag_run_id=dag_run_id)
        return REPORT_PATH
    except Exception as e:
        logger.error(f"Failed to generate report: {e}")
        log_pipeline_stage(engine, "report", "Failed", 0, str(e), dag_run_id=dag_run_id)
        raise

def get_html_template(**kwargs):
    # Format category rows
    cat_rows = ""
    for r in kwargs["categories"]:
        cat_rows += f"""
        <tr>
            <td class="font-medium text-white">{r['category']}</td>
            <td class="text-right">{r['total_books']}</td>
            <td class="text-right text-accent-cyan">£{r['average_price']:.2f}</td>
            <td class="text-right">
                <span class="rating-stars">{'★' * int(round(r['average_rating']))}</span>
                <span class="text-gray-500 font-mono">({r['average_rating']:.1f})</span>
            </td>
        </tr>
        """
    if not cat_rows:
        cat_rows = "<tr><td colspan='4' class='text-center text-gray-500'>No category metrics available</td></tr>"

    # Format author rows
    auth_rows = ""
    for r in kwargs["authors"]:
        auth_rows += f"""
        <tr>
            <td class="font-medium text-white">{r['author']}</td>
            <td class="text-right font-mono text-accent-purple font-bold">{r['total_quotes']}</td>
        </tr>
        """
    if not auth_rows:
        auth_rows = "<tr><td colspan='2' class='text-center text-gray-500'>No author metrics available</td></tr>"

    # Format log rows
    log_rows = ""
    for r in kwargs["logs"]:
        status_class = "status-success" if r['status'] == "Success" else "status-failure"
        # Handle timestamp display
        ts = r['timestamp']
        ts_str = ts.strftime("%Y-%m-%d %H:%M:%S") if hasattr(ts, "strftime") else str(ts)
        log_rows += f"""
        <tr>
            <td class="font-mono text-white text-xs">{r['stage']}</td>
            <td class="text-center">
                <span class="badge {status_class}">{r['status']}</span>
            </td>
            <td class="text-right font-mono">{r['record_count']}</td>
            <td class="text-right text-gray-400 font-mono text-xs">{ts_str}</td>
        </tr>
        """
    if not log_rows:
        log_rows = "<tr><td colspan='4' class='text-center text-gray-500'>No execution logs available</td></tr>"

    # Format reject books rows
    rej_books_rows = ""
    for r in kwargs["books_rejects"]:
        rej_books_rows += f"""
        <tr>
            <td class="font-medium text-white max-w-xs truncate" title="{r['title']}">{r['title']}</td>
            <td>{r['category']}</td>
            <td class="text-right font-mono text-xs">£{r['price']}</td>
            <td class="text-accent-red text-xs font-semibold">{r['rejection_reasons']}</td>
        </tr>
        """
    if not rej_books_rows:
        rej_books_rows = "<tr><td colspan='4' class='text-center text-success font-semibold py-4'>✓ No books rejected during this run!</td></tr>"

    # Format reject quotes rows
    rej_quotes_rows = ""
    for r in kwargs["quotes_rejects"]:
        rej_quotes_rows += f"""
        <tr>
            <td class="font-medium text-white max-w-md truncate" title="{r['quote']}">{r['quote']}</td>
            <td>{r['author']}</td>
            <td class="text-accent-red text-xs font-semibold">{r['rejection_reasons']}</td>
        </tr>
        """
    if not rej_quotes_rows:
        rej_quotes_rows = "<tr><td colspan='3' class='text-center text-success font-semibold py-4'>✓ No quotes rejected during this run!</td></tr>"

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Batch Pipeline Execution Summary - {kwargs['scrape_date']}</title>
    <!-- Google Fonts -->
    <link rel="preconnect" href="https://fonts.googleapis.com">
    <link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
    <link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600;700&family=Space+Grotesk:wght@400;500;600;700&display=swap" rel="stylesheet">
    
    <style>
        :root {{
            --bg-dark: #0a0b10;
            --panel-bg: rgba(18, 20, 32, 0.6);
            --border-color: rgba(255, 255, 255, 0.08);
            
            --accent-primary: #6366f1; /* Indigo */
            --accent-purple: #a855f7;
            --accent-cyan: #06b6d4;
            --accent-red: #ef4444;
            --accent-green: #10b981;
            
            --text-primary: #f3f4f6;
            --text-secondary: #9ca3af;
        }}

        * {{
            margin: 0;
            padding: 0;
            box-sizing: border-box;
        }}

        body {{
            background-color: var(--bg-dark);
            background-image: 
                radial-gradient(at 0% 0%, rgba(99, 102, 241, 0.1) 0px, transparent 50%),
                radial-gradient(at 100% 100%, rgba(168, 85, 247, 0.1) 0px, transparent 50%);
            color: var(--text-primary);
            font-family: 'Outfit', sans-serif;
            min-height: 100vh;
            padding: 2rem 1.5rem;
            line-height: 1.5;
        }}

        h1, h2, h3, h4 {{
            font-family: 'Space Grotesk', sans-serif;
            font-weight: 700;
        }}

        .container {{
            max-width: 1200px;
            margin: 0 auto;
        }}

        /* Header Styling */
        header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 2.5rem;
            border-bottom: 1px solid var(--border-color);
            padding-bottom: 1.5rem;
        }}

        .title-area h1 {{
            font-size: 2.2rem;
            background: linear-gradient(135deg, #fff 0%, #a855f7 100%);
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
            letter-spacing: -0.5px;
        }}

        .title-area p {{
            color: var(--text-secondary);
            font-size: 0.95rem;
            margin-top: 0.25rem;
        }}

        .meta-badge {{
            background: rgba(99, 102, 241, 0.1);
            border: 1px solid rgba(99, 102, 241, 0.2);
            border-radius: 99px;
            padding: 0.5rem 1.2rem;
            display: flex;
            flex-direction: column;
            align-items: flex-end;
        }}

        .meta-badge .run-id {{
            font-family: monospace;
            font-size: 0.8rem;
            color: var(--accent-cyan);
        }}

        .meta-badge .run-date {{
            font-size: 0.9rem;
            font-weight: 600;
        }}

        /* Grid Layouts */
        .metrics-grid {{
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(240px, 1fr));
            gap: 1.5rem;
            margin-bottom: 2.5rem;
        }}

        .dashboard-grid {{
            display: grid;
            grid-template-columns: 2fr 1fr;
            gap: 2rem;
            margin-bottom: 2.5rem;
        }}

        @media (max-width: 900px) {{
            .dashboard-grid {{
                grid-template-columns: 1fr;
            }}
        }}

        /* Glassmorphic Panel */
        .panel {{
            background: var(--panel-bg);
            backdrop-filter: blur(16px);
            -webkit-backdrop-filter: blur(16px);
            border: 1px solid var(--border-color);
            border-radius: 16px;
            padding: 1.5rem;
            box-shadow: 0 8px 32px 0 rgba(0, 0, 0, 0.37);
            transition: transform 0.2s ease, box-shadow 0.2s ease;
        }}

        .panel:hover {{
            box-shadow: 0 8px 32px 0 rgba(99, 102, 241, 0.05);
        }}

        .panel-header {{
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 1.25rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.05);
            padding-bottom: 0.75rem;
        }}

        .panel-header h2 {{
            font-size: 1.25rem;
            font-weight: 600;
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        .panel-header h2::before {{
            content: '';
            display: inline-block;
            width: 4px;
            height: 18px;
            background: var(--accent-primary);
            border-radius: 2px;
        }}

        /* Stat Cards */
        .stat-card {{
            position: relative;
            overflow: hidden;
        }}

        .stat-card::after {{
            content: '';
            position: absolute;
            top: 0;
            left: 0;
            width: 100%;
            height: 4px;
        }}

        .stat-card.scraped::after {{ background: var(--accent-cyan); }}
        .stat-card.clean::after {{ background: var(--accent-green); }}
        .stat-card.rejected::after {{ background: var(--accent-red); }}

        .stat-label {{
            font-size: 0.85rem;
            text-transform: uppercase;
            letter-spacing: 1px;
            color: var(--text-secondary);
            font-weight: 500;
        }}

        .stat-value {{
            font-size: 2.5rem;
            font-weight: 700;
            font-family: 'Space Grotesk', sans-serif;
            line-height: 1.2;
            margin: 0.25rem 0;
        }}

        .stat-details {{
            font-size: 0.8rem;
            color: var(--text-secondary);
            display: flex;
            justify-content: space-between;
            margin-top: 0.5rem;
            border-top: 1px solid rgba(255, 255, 255, 0.03);
            padding-top: 0.5rem;
        }}

        /* Table Styling */
        table {{
            width: 100%;
            border-collapse: collapse;
            text-align: left;
            font-size: 0.9rem;
        }}

        th {{
            font-family: 'Space Grotesk', sans-serif;
            color: var(--text-secondary);
            font-weight: 600;
            padding: 0.75rem 1rem;
            border-bottom: 2px solid rgba(255, 255, 255, 0.05);
            text-transform: uppercase;
            font-size: 0.75rem;
            letter-spacing: 0.5px;
        }}

        td {{
            padding: 0.85rem 1rem;
            border-bottom: 1px solid rgba(255, 255, 255, 0.03);
            color: var(--text-secondary);
        }}

        tr:hover td {{
            color: var(--text-primary);
            background: rgba(255, 255, 255, 0.01);
        }}

        /* Helpers & Badges */
        .text-right {{ text-align: right; }}
        .text-center {{ text-align: center; }}
        .font-mono {{ font-family: monospace; }}
        .font-medium {{ font-weight: 500; }}
        .text-white {{ color: #fff; }}
        .text-accent-cyan {{ color: var(--accent-cyan); }}
        .text-accent-purple {{ color: var(--accent-purple); }}
        .text-accent-red {{ color: var(--accent-red); }}
        .text-success {{ color: var(--accent-green); }}

        .badge {{
            display: inline-block;
            padding: 0.25rem 0.6rem;
            border-radius: 6px;
            font-size: 0.75rem;
            font-weight: 600;
            text-transform: uppercase;
            letter-spacing: 0.5px;
        }}

        .status-success {{
            background: rgba(16, 185, 129, 0.1);
            color: var(--accent-green);
            border: 1px solid rgba(16, 185, 129, 0.2);
        }}

        .status-failure {{
            background: rgba(239, 68, 68, 0.1);
            color: var(--accent-red);
            border: 1px solid rgba(239, 68, 68, 0.2);
        }}

        .rating-stars {{
            color: #fbbf24; /* Amber star colors */
            font-size: 1rem;
            letter-spacing: -2px;
        }}

        /* Reject details */
        .reject-table-section {{
            margin-bottom: 2rem;
        }}

        .reject-table-section h3 {{
            font-size: 1.05rem;
            margin-bottom: 0.75rem;
            color: var(--text-primary);
            display: flex;
            align-items: center;
            gap: 0.5rem;
        }}

        .reject-table-section h3::after {{
            content: '';
            flex-grow: 1;
            height: 1px;
            background: rgba(255,255,255,0.05);
            margin-left: 0.5rem;
        }}

        .max-w-xs {{ max-width: 200px; }}
        .max-w-md {{ max-width: 350px; }}
        .truncate {{
            overflow: hidden;
            text-overflow: ellipsis;
            white-space: nowrap;
        }}
    </style>
</head>
<body>
    <div class="container">
        <!-- Header -->
        <header>
            <div class="title-area">
                <h1>Batch Pipeline Summary</h1>
                <p>Cloud-First Data Engineer Weekly Technical Breakdown - Week 1</p>
            </div>
            <div class="meta-badge">
                <span class="run-date">{kwargs['scrape_date']}</span>
                <span class="run-id">ID: {kwargs['dag_run_id']}</span>
            </div>
        </header>

        <!-- KPI Cards -->
        <section class="metrics-grid">
            <!-- Scraped Card -->
            <div class="panel stat-card scraped">
                <div class="stat-label">Total Records Scraped</div>
                <div class="stat-value text-accent-cyan">{kwargs['total_scraped']}</div>
                <div class="stat-details">
                    <span>Books: {kwargs['raw_books'] + kwargs['rejected_books']}</span>
                    <span>Quotes: {kwargs['raw_quotes'] + kwargs['rejected_quotes']}</span>
                </div>
            </div>

            <!-- Clean / Ingested Card -->
            <div class="panel stat-card clean">
                <div class="stat-label">Validated &amp; Loaded</div>
                <div class="stat-value text-success">{kwargs['total_clean']}</div>
                <div class="stat-details">
                    <span>Books: {kwargs['clean_books']}</span>
                    <span>Quotes: {kwargs['clean_quotes']}</span>
                </div>
            </div>

            <!-- Rejected Card -->
            <div class="panel stat-card rejected">
                <div class="stat-label">Quarantined / Rejected</div>
                <div class="stat-value text-accent-red">{kwargs['total_rejected']}</div>
                <div class="stat-details">
                    <span>Books: {kwargs['rejected_books']}</span>
                    <span>Quotes: {kwargs['rejected_quotes']}</span>
                </div>
            </div>
        </section>

        <!-- Mid section: Category analytical tables and Author stats -->
        <section class="dashboard-grid">
            <!-- Left panel: Book Categories -->
            <div class="panel">
                <div class="panel-header">
                    <h2>Top Book Categories</h2>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Category</th>
                            <th class="text-right">Books Count</th>
                            <th class="text-right">Average Price</th>
                            <th class="text-right">Average Rating</th>
                        </tr>
                    </thead>
                    <tbody>
                        {cat_rows}
                    </tbody>
                </table>
            </div>

            <!-- Right panel: Top Authors -->
            <div class="panel">
                <div class="panel-header">
                    <h2>Top Quoted Authors</h2>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Author</th>
                            <th class="text-right">Quotes</th>
                        </tr>
                    </thead>
                    <tbody>
                        {auth_rows}
                    </tbody>
                </table>
            </div>
        </section>

        <!-- Lower Section: Execution Logs and Quarantine summary -->
        <section class="dashboard-grid">
            <!-- Left: Execution Log -->
            <div class="panel">
                <div class="panel-header">
                    <h2>Pipeline Execution Logs</h2>
                </div>
                <table>
                    <thead>
                        <tr>
                            <th>Pipeline Stage</th>
                            <th class="text-center">Status</th>
                            <th class="text-right">Records</th>
                            <th class="text-right">Timestamp</th>
                        </tr>
                    </thead>
                    <tbody>
                        {log_rows}
                    </tbody>
                </table>
            </div>

            <!-- Right: Quarantine Summary Panel -->
            <div class="panel">
                <div class="panel-header">
                    <h2>Quarantine (Reject Samples)</h2>
                </div>
                
                <div class="reject-table-section">
                    <h3>Rejected Books Sample ({kwargs['rejected_books']} total)</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>Title</th>
                                <th>Category</th>
                                <th class="text-right">Price</th>
                                <th>Reason</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rej_books_rows}
                        </tbody>
                    </table>
                </div>

                <div class="reject-table-section">
                    <h3>Rejected Quotes Sample ({kwargs['rejected_quotes']} total)</h3>
                    <table>
                        <thead>
                            <tr>
                                <th>Quote</th>
                                <th>Author</th>
                                <th>Reason</th>
                            </tr>
                        </thead>
                        <tbody>
                            {rej_quotes_rows}
                        </tbody>
                    </table>
                </div>
            </div>
        </section>
    </div>
</body>
</html>
"""
    return html

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    # Test execution
    generate_html_report()
