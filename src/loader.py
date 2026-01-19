import os
import json
import glob
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")

def get_db_connection():
    """Constructs the database URL and returns a SQLAlchemy engine."""
    if not all([DB_NAME, DB_USER, DB_PASSWORD]):
        raise ValueError("Database credentials not fully set in .env")
    
    db_url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    return create_engine(db_url)

def load_data():
    """Reads JSON files and loads them into PostgreSQL."""
    engine = get_db_connection()
    
    # Create 'raw' schema
    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS raw;"))
        conn.commit()
    
    # Find all JSON files
    base_path = "data/raw/telegram_messages"
    json_files = glob.glob(os.path.join(base_path, "**", "*.json"), recursive=True)
    
    if not json_files:
        print("No JSON files found in data/raw/telegram_messages.")
        return

    all_data = []
    print(f"Found {len(json_files)} JSON files.")
    
    for file_path in json_files:
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    all_data.append(data)
        except Exception as e:
            print(f"Error reading {file_path}: {e}")

    if not all_data:
        print("No data extracted from files.")
        return

    df = pd.DataFrame(all_data)
    
    # Load to PostgreSQL
    # Using 'replace' to overwrite the table for idempotent runs during development
    # In production, 'append' with de-duplication would be better
    table_name = 'telegram_messages'
    schema = 'raw'
    
    print(f"Loading {len(df)} rows into {schema}.{table_name}...")
    
    try:
        df.to_sql(table_name, engine, schema=schema, if_exists='replace', index=False)
        print("Data loaded successfully.")
    except Exception as e:
        print(f"Error loading data to Postgres: {e}")

if __name__ == "__main__":
    load_data()
