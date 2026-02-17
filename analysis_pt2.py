print("1")
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

import os
import logging

from db.init_db import init_db
from db.connection import connect_to_db

def setup_logging(log_level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)

    logging.basicConfig(
        filename="logs/analysis.log",
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )

def main() -> None:
    setup_logging("INFO")
    logging.info("Starting analysis")

    init_db(reset=True) 

    conn = connect_to_db()
    cur = conn.cursor()
    print("2")
    try:
        # TODO: Add analysis stuff here
        # two feature engineering examples
        # one correlation
        # three visualizations

        # 
        df = pd.read_sql("SELECT * FROM stg_air_quality_ny;", conn)
        logging.info(f"Loaded DataFrame with shape {df.shape}")


        # correlate the time with the air quality index
        df['start_date'] = pd.to_datetime(df['start_date'])
        df['hour'] = df['start_date'].dt.hour
        correlation = df['hour'].corr(df['Data Value'])
        print(f"Correlation between hour and air quality index: {correlation:.2f}")
        logging.info(f"Correlation between hour and air quality index: {correlation:.2f}")



    except Exception as e:
        conn.rollback()
        logging.error(f"Database connection failed during analysis: {e}")
        raise

    # close connections
    finally:
        cur.close()
        conn.close()
