import sqlite3
import pandas as pd
from pathlib import Path

STAGING_PATH = Path("data/Staging")
TRANSFORMATION_PATH = Path("data/Transformation")
TRANSFORMATION_PATH.mkdir(parents=True, exist_ok=True)

JPY_PER_USD = 150  # fixed conversion

def transform_store(staging_db, country):
    staging_conn = sqlite3.connect(STAGING_PATH / staging_db)
    transform_conn = sqlite3.connect(TRANSFORMATION_PATH / "transformation layer.db")

    sales = pd.read_sql("SELECT * FROM sales_data", staging_conn)
    items = pd.read_sql(f"SELECT * FROM {country}_items", staging_conn)

    # Clean column names (remove quotes + normalize)
    sales.columns = sales.columns.str.replace("'", "").str.lower()
    items.columns = items.columns.str.lower()

    # Remove nulls
    sales.dropna(inplace=True)
    items.dropna(inplace=True)

    # Merge
    df = sales.merge(
        items,
        left_on="product_id",
        right_on="id",
        how="left"
    )

    # Currency standardization
    if country == "japan":
        df["price_usd"] = df["price"] / JPY_PER_USD
    else:
        df["price_usd"] = df["price"]

    df["country"] = country.capitalize()

    # Save transformed data
    df.to_sql(
        f"{country}_transformed",
        transform_conn,
        if_exists="replace",
        index=False
    )

    staging_conn.close()
    transform_conn.close()

if __name__ == "__main__":
    transform_store("japan staging area.db", "japan")
    transform_store("myanmar staging area.db", "myanmar")