import json
import math
from typing import List, Dict, Any, Tuple
from datetime import datetime

from psycopg2.extras import execute_batch
from db.connection import connect_to_db

# --- DATABASE COLUMN DEFINITIONS ---

# Columns to INSERT into
# exclude autoincrements aka primary keys
# TODO: we're not really sending a status code nor a potentinal error message, may need logic handling that
INGESTION_RUNS_INSERT_COLS = [
    "source_file",
    "start_timestamp",
    "end_timestamp",
    "total_records",
    "valid_records",
    "rejected_records",
]

INGESTION_REJECTS_COLS = [
    "run_id",
    "raw_record",
    "error_reason",
    "source_file",
]

# Dimension: Indicators
INDICATORS_COLS = [
    "indicator_id",
    "name",
    "measure",
    "measure_info",
]

# Dimension: Geographic
GEOGRAPHIC_COLS = [
    "geo_join_id",
    "geo_type_name",
    "geo_place_name",
]

# Fact: Measurements
MEASUREMENTS_COLS = [
    "unique_id",
    "indicator_id",
    "geo_join_id",
    "time_period",
    "start_date",
    "data_value",
]

INSERT_INDICATORS = """
INSERT INTO indicators (indicator_id, name, measure, measure_info)
VALUES (%(indicator_id)s, %(name)s, %(measure)s, %(measure_info)s)
ON CONFLICT (indicator_id) DO NOTHING;
"""

INSERT_GEOGRAPHIC = """
INSERT INTO geographic (geo_join_id, geo_type_name, geo_place_name)
VALUES (%(geo_join_id)s, %(geo_type_name)s, %(geo_place_name)s)
ON CONFLICT (geo_join_id) DO NOTHING;
"""

# Return 1 only when an insert actually happens (duplicates return 0 rows)
INSERT_MEASUREMENTS = """
INSERT INTO measurements (
    unique_id,
    indicator_id,
    geo_join_id,
    time_period,
    start_date,
    data_value,
    message,
    run_id
)
VALUES (
    %(unique_id)s,
    %(indicator_id)s,
    %(geo_join_id)s,
    %(time_period)s,
    %(start_date)s,
    %(data_value)s,
    %(message)s,
    %(run_id)s
)
ON CONFLICT (unique_id) DO NOTHING
RETURNING 1;
"""


INSERT_REJECT = """
INSERT INTO ingestion_rejects (run_id, raw_record, error_reason, source_file)
VALUES (%s, %s, %s, %s)
RETURNING 1;
"""


# -----------------------
# Helpers
# -----------------------

def sanitize_for_json(value: Any) -> Any:
    """Convert NaN to None so json.dumps(allow_nan=False) won't crash."""
    if isinstance(value, float) and math.isnan(value):
        return None
    if isinstance(value, dict):
        return {k: sanitize_for_json(v) for k, v in value.items()}
    if isinstance(value, list):
        return [sanitize_for_json(v) for v in value]
    return value


def build_insert_sql(
    table_name: str, columns: List[str], conflict_target: str = None
) -> str:
    """
    Builds a dynamic SQL insert statement.
    If conflict_target is provided, adds 'ON CONFLICT DO NOTHING'.
    """
    cols = ", ".join(columns)
    vals = ", ".join([f"%({c})s" for c in columns])

    sql = f"INSERT INTO {table_name} ({cols}) VALUES ({vals})"

    if conflict_target:
        sql += f" ON CONFLICT ({conflict_target}) DO NOTHING"

    return sql


def extract_dimension_data(
    records: List[Dict], key_map: Dict[str, str], unique_key: str
) -> List[Dict]:
    """
    Extracts unique dimension data (e.g., unique Indicators) from the raw list of records.
    key_map: maps DB column name -> CSV/Source key name
    """
    seen = set()
    unique_rows = []

    for r in records:
        # Create a tuple of the ID to check for uniqueness
        rec_id = r.get(key_map[unique_key])

        if rec_id and rec_id not in seen:
            seen.add(rec_id)
            # Build the dictionary for this row based on the mapping
            row = {}
            for db_col, source_key in key_map.items():
                row[db_col] = r.get(source_key)
            unique_rows.append(row)

    return unique_rows

def map_measurement(record: Dict, run_id: int) -> Dict:
    return {
        "unique_id": record.get("unique_id"),
        "indicator_id": record.get("indicator_id"),
        "geo_join_id": record.get("geo_join_id"),
        "time_period": record.get("time_period"),
        "start_date": record.get("start_date"),
        "data_value": record.get("data_value"),
        "message": record.get("message"),
        "run_id": run_id,
    }


# -----------------------
# Main loader
# -----------------------

def load_records(
    valid_records: List[Dict],
    rejected_records: List[Dict],
    source_file: str,
    ingestion_runs_table: str = "ingestion_runs",
    ingestion_reject_table: str = "ingestion_rejects",
    measurements_table: str = "measurements",
    indicators_table: str = "indicators",
    geographic_table: str = "geographic",
    batch_size: int = 500,
) -> None:

    conn = connect_to_db()
    cur = conn.cursor()

    try:
        # 1. LOG THE RUN (Get run_id)
        # ---------------------------------------------------------
        run_insert_sql = f"""
            INSERT INTO {ingestion_runs_table} 
            ({', '.join(INGESTION_RUNS_INSERT_COLS)}) 
            VALUES (%s, %s, %s, %s, %s, %s) 
            RETURNING run_id;
        """
        time_before = datetime.now()

        cur.execute(
            run_insert_sql,
            (
                source_file,
                time_before,  # start_timestamp
                datetime.now(),  # end_timestamp
                len(valid_records) + len(rejected_records),
                len(valid_records),
                len(rejected_records),
            ),
        )
        run_id = cur.fetchone()[0]
        print(f"Run ID generated: {run_id}")

        # 2. PREPARE & LOAD DIMENSIONS (Indicators)
        # ---------------------------------------------------------
        # Mapping: DB Column -> Source CSV Header
        indicator_map = {
            "indicator_id": "indicator_id",
            "name": "name",
            "measure": "measure",
            "measure_info": "measure_info",
        }
        unique_indicators = extract_dimension_data(
            valid_records, indicator_map, "indicator_id"
        )

        if unique_indicators:
            sql = build_insert_sql(
                indicators_table, INDICATORS_COLS, conflict_target="indicator_id"
            )
            execute_batch(cur, sql, unique_indicators, page_size=batch_size)

        # 3. PREPARE & LOAD DIMENSIONS (Geographic)
        # ---------------------------------------------------------
        geo_map = {
            "geo_join_id": "geo_join_id",
            "geo_type_name": "geo_type_name",
            "geo_place_name": "geo_place_name",
        }
        unique_geo = extract_dimension_data(valid_records, geo_map, "geo_join_id")

        if unique_geo:
            sql = build_insert_sql(
                geographic_table, GEOGRAPHIC_COLS, conflict_target="geo_join_id"
            )
            execute_batch(cur, sql, unique_geo, page_size=batch_size)

        # 4. LOAD MEASUREMENTS (Facts)
        # ---------------------------------------------------------
        # 4. LOAD MEASUREMENTS (Facts)
        # ---------------------------------------------------------
        measurements_data = []
        for r in valid_records:
            measurements_data.append(
                {
                    "unique_id": r.get("unique_id"),
                    "indicator_id": r.get("indicator_id"),
                    "geo_join_id": r.get("geo_join_id"),
                    "time_period": r.get("time_period"),
                    "start_date": r.get("start_date"),
                    "data_value": r.get("data_value"),
                    "message": r.get("message"),  # Added to match your INSERT statement
                    "run_id": run_id              # Added to match your INSERT statement
                }
            )

        if measurements_data:
            # Note: unique_id is likely the PK, so we might need conflict handling here too
            # depending on if you are reloading the same file.
            sql = build_insert_sql(
                measurements_table, MEASUREMENTS_COLS, conflict_target="unique_id"
            )
            execute_batch(cur, sql, measurements_data, page_size=batch_size)

        # 5. LOAD REJECTS
        # ---------------------------------------------------------
        reject_rows = []
        for r in rejected_records:
            sanitized = sanitize_for_json(r)
            # Remove error_reason from the raw dump to keep it clean, if desired
            error_reason = sanitized.pop("error_reason", "Unknown validation error")

            reject_rows.append(
                {
                    "run_id": run_id,
                    "raw_record": json.dumps(
                        sanitized, default=str
                    ),  # default=str handles dates
                    "error_reason": error_reason,
                    "source_file": source_file,
                }
            )

        rejects_inserted = 0
        if reject_rows:
            sql = build_insert_sql(ingestion_reject_table, INGESTION_REJECTS_COLS)
            execute_batch(cur, sql, reject_rows, page_size=batch_size)

        conn.commit()
        print("Batch load committed successfully.")

    except Exception as e:
        conn.rollback()
        print(f"Error during loading: {e}")
        raise
    finally:
        cur.close()
        conn.close()
