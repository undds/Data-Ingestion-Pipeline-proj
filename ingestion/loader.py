import math
import json
from typing import List, Dict, Any

from psycopg2.extras import execute_batch
from db.connection import connect_to_db


# columns for stg_air_quality_ny
AIR_QUALITY_COLUMNS = [
    "unique_id",
    "indicator_id",
    "name",
    "measure",
    "measure_info",
    "geo_type_name",
    "geo_join_id",
    "geo_place_name",
    "time_period",
    "start_date",
    "data_value",
    "message",
    "source_file",
]


def sanitize_for_json(value: Any) -> Any:
    """Convert NaN values to None for JSON compatibility."""
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, dict):
        return {k: sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_for_json(v) for v in value]
    return value


def normalize_record(record: Dict, source_file: str) -> Dict:
    """
    Map incoming records to DB schema safely.
    Uses .get() to avoid KeyError and ensures expected keys exist.
    """
    mapped = {col: record.get(col) for col in AIR_QUALITY_COLUMNS if col != "source_file"}
    mapped["source_file"] = source_file
    return mapped


def build_insert_sql(table_name: str, columns: List[str]) -> str:
    cols = ",\n    ".join(columns)
    vals = ",\n    ".join([f"%({c})s" for c in columns])

    return f"""
INSERT INTO {table_name} (
    {cols}
) VALUES (
    {vals}
);
""".strip()


def load_records(
    valid_records: List[Dict],
    rejected_records: List[Dict],
    source_file: str,
    target_table: str,
    reject_table: str,
    batch_size: int = 500,
) -> None:
    """
    Load valid and rejected records into PostgreSQL.

    - Valid records -> target_table
    - Rejected records -> reject_table (raw_record + error_reason + source_file)
    """

    insert_valid_sql = build_insert_sql(target_table, AIR_QUALITY_COLUMNS)

    insert_reject_sql = f"""
INSERT INTO {reject_table} (
    raw_record,
    error_reason,
    source_file
) VALUES (%s, %s, %s);
""".strip()

    conn = connect_to_db()
    cur = conn.cursor()

    try:
        # -------- VALID RECORDS (BATCH INSERT) --------
        mapped_records = [normalize_record(r, source_file) for r in valid_records]

        if mapped_records:
            execute_batch(cur, insert_valid_sql, mapped_records, page_size=batch_size)

        # -------- REJECTED RECORDS (BATCH INSERT) --------
        reject_rows = []
        for r in rejected_records:
            sanitized = sanitize_for_json(r)
            error_reason = sanitized.get("error_reason", "Validation failed")
            reject_rows.append(
                (
                    json.dumps(sanitized, allow_nan=False),
                    error_reason,
                    source_file,
                )
            )

        if reject_rows:
            execute_batch(cur, insert_reject_sql, reject_rows, page_size=batch_size)

        conn.commit()

    except Exception:
        conn.rollback()
        raise

    finally:
        cur.close()
        conn.close()
