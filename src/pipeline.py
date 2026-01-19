import os
import subprocess
from dagster import job, op, ScheduleDefinition, get_dagster_logger

logger = get_dagster_logger()

# Define paths
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SRC_DIR = os.path.join(BASE_DIR, 'src')
DBT_DIR = os.path.join(BASE_DIR, 'medical_warehouse')

@op
def scrape_telegram_data():
    """Runs the Telethon scraper script."""
    logger.info("Starting Telegram Scraper...")
    try:
        # Running as subprocess to avoid asyncio conflict with Dagster/Telethon
        result = subprocess.run(
            ['python3', 'src/scraper.py'], 
            cwd=BASE_DIR, 
            capture_output=True, 
            text=True
        )
        if result.returncode != 0:
             raise Exception(f"Scraper failed: {result.stderr}")
        logger.info(f"Scraper output: {result.stdout}")
    except Exception as e:
        logger.error(f"Scraping error: {str(e)}")
        raise

@op
def load_raw_to_postgres():
    """Runs the data loader to Postgres."""
    logger.info("Loading data to PostgreSQL...")
    try:
        result = subprocess.run(
            ['python3', 'src/loader.py'], 
            cwd=BASE_DIR, 
            capture_output=True, 
            text=True
        )
        if result.returncode != 0:
             raise Exception(f"Loader failed: {result.stderr}")
        logger.info(f"Loader output: {result.stdout}")
    except Exception as e:
        logger.error(f"Loading error: {str(e)}")
        raise

@op
def run_yolo_enrichment():
    """Runs YOLOv8 object detection."""
    logger.info("Running YOLO Enrichment...")
    try:
        result = subprocess.run(
            ['python3', 'src/yolo_detect.py'], 
            cwd=BASE_DIR, 
            capture_output=True, 
            text=True
        )
        if result.returncode != 0:
             raise Exception(f"YOLO detection failed: {result.stderr}")
        logger.info(f"YOLO output: {result.stdout}")
    except Exception as e:
        logger.error(f"Enrichment error: {str(e)}")
        raise

@op
def run_dbt_transformations():
    """Runs dbt run and dbt test."""
    logger.info("Running dbt transformations...")
    
    # dbt run
    run_res = subprocess.run(
        ['dbt', 'run'], 
        cwd=DBT_DIR, 
        capture_output=True, 
        text=True
    )
    if run_res.returncode != 0:
        logger.error(f"dbt run failed: {run_res.stderr}")
        raise Exception("dbt run failed")
    logger.info(f"dbt run output: {run_res.stdout}")

    # dbt test
    test_res = subprocess.run(
        ['dbt', 'test'], 
        cwd=DBT_DIR, 
        capture_output=True, 
        text=True
    )
    if test_res.returncode != 0:
        logger.error(f"dbt test failed: {test_res.stderr}")
        raise Exception("dbt test failed")
    logger.info(f"dbt test output: {test_res.stdout}")

@job
def medical_pipeline_job():
    """Defines the full pipeline execution order."""
    scraped = scrape_telegram_data()
    loaded = load_raw_to_postgres(scraped) # wait for scrape
    enriched = run_yolo_enrichment(loaded) # wait for load
    run_dbt_transformations(enriched) # wait for enrich

# Schedule the job to run daily at midnight
daily_schedule = ScheduleDefinition(
    job=medical_pipeline_job,
    cron_schedule="0 0 * * *", 
)
