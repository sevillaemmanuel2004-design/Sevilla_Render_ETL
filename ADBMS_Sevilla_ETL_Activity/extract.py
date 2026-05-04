import os
import pandas as pd
from pathlib import Path
from sqlalchemy import create_engine, text

# Base paths
SOURCE_PATH = Path("data/source")

# PostgreSQL connection from environment variable
DATABASE_URL = os.environ["DATABASE_URL"]

def get_engine():
    return create_engine(DATABASE_URL)

def extract_store(store_name, schema_prefix):
    engine = get_engine()
    store_path = SOURCE_PATH / store_name

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS staging"))
        conn.commit()

    for csv_file in store_path.glob("*.csv"):
        table_name = f"{schema_prefix}_{csv_file.stem.lower().replace(' ', '_')}"
        df = pd.read_csv(csv_file)
        df.to_sql(
            table_name,
            engine,
            schema="staging",
            if_exists="replace",
            index=False
        )
        print(f"Loaded {csv_file.name} → staging.{table_name}")

    engine.dispose()

if __name__ == "__main__":
    extract_store("japan_store", "japan")
    extract_store("myanmar_store", "myanmar")
