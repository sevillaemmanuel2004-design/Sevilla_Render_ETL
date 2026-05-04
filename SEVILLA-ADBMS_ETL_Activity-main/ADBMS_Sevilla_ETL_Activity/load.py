import os
import pandas as pd
from sqlalchemy import create_engine, text

DATABASE_URL = os.environ["DATABASE_URL"]

def get_engine():
    return create_engine(DATABASE_URL)

def load_to_presentation():
    engine = get_engine()

    with engine.connect() as conn:
        conn.execute(text("CREATE SCHEMA IF NOT EXISTS presentation"))
        conn.commit()

    japan = pd.read_sql("SELECT * FROM transformation.japan_transformed", engine)
    myanmar = pd.read_sql("SELECT * FROM transformation.myanmar_transformed", engine)

    big_table = pd.concat([japan, myanmar], ignore_index=True)

    big_table.to_sql(
        "consolidated_sales",
        engine,
        schema="presentation",
        if_exists="replace",
        index=False
    )

    print(f"Final consolidated table created → presentation.consolidated_sales ({len(big_table)} rows)")
    engine.dispose()

if __name__ == "__main__":
    load_to_presentation()
