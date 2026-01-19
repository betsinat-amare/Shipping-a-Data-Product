# Medical Telegram Warehouse

End-to-end data pipeline for Telegram data analysis of Ethiopian medical businesses.

## Structure
- `data/`: Raw and processed data
- `src/`: Source code for scrapers and utils
- `medical_warehouse/`: dbt project
- `api/`: FastAPI application
- `notebooks/`: Jupyter notebooks for analysis

## Setup
1. Install dependencies: `pip install -r requirements.txt`
2. Setup `.env` with Telegram credentials.
3. Run scraper: `python src/scraper.py`
