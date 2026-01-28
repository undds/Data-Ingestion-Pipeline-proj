import logging
import pandas as pd
from typing import List, Dict


def read_csv(file_path: str) -> List[Dict]:
    """
    Read a CSV file and return a list of records as dictionaries.

    Args:
        file_path (str): Path to the CSV file

    Returns:
        List[Dict]: List of row-level records
    """
    try:
        df = pd.read_csv(file_path)

        df.columns = (
            df.columns
            .str.strip()
            .str.lower()
            .str.replace(" ", "_")
        )

        records = df.to_dict(orient="records")
        logging.info(f"Read {len(records)} records from {file_path}")
        return records

    except Exception as e:
        logging.error(f"Failed to read CSV: {e}")
        raise