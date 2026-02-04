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

    init_db()

    conn = connect_to_db()
    cur = conn.cursor()

    try:
        # TODO: Add analysis stuff here
        cur.execute("SELECT * FROM stg_air_quality_ny;")

    except Exception as e:
        conn.rollback()
        logging.error(f"Database connection failed during analysis: {e}")
        raise

    # close connections
    finally:
        cur.close()
        conn.close()
