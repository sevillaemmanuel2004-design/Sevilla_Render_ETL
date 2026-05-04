import sqlite3
import pandas as pd
from pathlib import Path

# Base paths
SOURCE_PATH = Path("data/source")
STAGING_PATH = Path("data/Staging")

# Ensure staging folder exists
STAGING_PATH.mkdir(parents=True, exist_ok=True)

def extract_store(store_name, db_name):
    db_path = STAGING_PATH / db_name
    conn = sqlite3.connect(db_path)

    store_path = SOURCE_PATH / store_name

    for csv_file in store_path.glob("*.csv"):
        table_name = csv_file.stem
        df = pd.read_csv(csv_file)

        df.to_sql(table_name, conn, if_exists="replace", index=False)
        print(f"Loaded {table_name} into {db_name}")

    conn.close()

if __name__ == "__main__":
    extract_store("japan_store", "japan staging area.db")
    extract_store("myanmar_store", "myanmar staging area.db")