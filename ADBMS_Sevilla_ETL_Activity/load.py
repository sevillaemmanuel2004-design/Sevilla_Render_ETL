import sqlite3
import pandas as pd
from pathlib import Path

TRANSFORMATION_PATH = Path("data/Transformation")
PRESENTATION_PATH = Path("data/Presentation")
PRESENTATION_PATH.mkdir(parents=True, exist_ok=True)

def load_to_presentation():
    transform_conn = sqlite3.connect(
        TRANSFORMATION_PATH / "transformation layer.db"
    )
    presentation_conn = sqlite3.connect(
        PRESENTATION_PATH / "BIG TABLE.db"
    )

    japan = pd.read_sql("SELECT * FROM japan_transformed", transform_conn)
    myanmar = pd.read_sql("SELECT * FROM myanmar_transformed", transform_conn)

    big_table = pd.concat([japan, myanmar], ignore_index=True)

    big_table.to_sql(
        "consolidated_sales",
        presentation_conn,
        if_exists="replace",
        index=False
    )

    transform_conn.close()
    presentation_conn.close()

    print("Final consolidated table created.")

if __name__ == "__main__":
    load_to_presentation()
