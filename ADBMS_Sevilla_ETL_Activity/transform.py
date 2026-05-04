import os
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]
JPY_PER_USD = 150  # fixed conversion

def get_engine():
    return create_engine(DATABASE_URL)

def transform_store(country):
    engine = get_engine()

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS transformation"))
        conn.commit()

    # Table names are derived from CSV filenames: {prefix}_{stem_lowercase}
    # sales_data.csv -> {country}_sales_data
    # japan_items.csv -> japan_japan_items  |  myanmar_items.csv -> myanmar_myanmar_items
    sales = pd.read_sql(
        f"SELECT * FROM staging.{country}_sales_data", engine
    )
    items = pd.read_sql(
        f"SELECT * FROM staging.{country}_{country}_items", engine
    )

    # Clean column names
    sales.columns = sales.columns.str.replace("'", "").str.lower()
    items.columns = items.columns.str.lower()

    # Remove nulls
    sales.dropna(inplace=True)
    items.dropna(inplace=True)

    # Merge on product_id / id
    df = sales.merge(items, left_on="product_id", right_on="id", how="left")

    # Currency standardization
    if country == "japan":
        df["price_usd"] = df["price"] / JPY_PER_USD
    else:
        df["price_usd"] = df["price"]

    df["country"] = country.capitalize()

    df.to_sql(
        f"{country}_transformed",
        engine,
        schema="transformation",
        if_exists="replace",
        index=False
    )

    print(f"Transformed {country} → transformation.{country}_transformed ({len(df)} rows)")
    engine.dispose()

if __name__ == "__main__":
    transform_store("japan")
    transform_store("myanmar")
