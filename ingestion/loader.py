import json
import math
from typing import List, Dict, Any, Tuple

from psycopg2.extras import execute_batch
from db.connection import connect_to_db


# -----------------------
# SQL (normalized schema)
# -----------------------

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


def map_indicator(record: Dict) -> Dict:
    return {
        "indicator_id": record.get("indicator_id"),
        "name": record.get("name"),
        "measure": record.get("measure"),
        "measure_info": record.get("measure_info"),
    }


def map_geographic(record: Dict) -> Dict:
    return {
        "geo_join_id": record.get("geo_join_id"),
        "geo_type_name": record.get("geo_type_name"),
        "geo_place_name": record.get("geo_place_name"),
    }


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
    run_id: int,
    batch_size: int = 500,
) -> Tuple[int, int]:
    """
    Load records into normalized tables.

    Returns:
        (measurements_inserted, rejects_inserted)
    """

    conn = connect_to_db()
    cur = conn.cursor()

    try:
        # 1) Insert dimensions (dedupe in Python to reduce DB work)
        indicators: dict[int, Dict] = {}
        geos: dict[int, Dict] = {}

        for r in valid_records:
            ind = map_indicator(r)
            if ind["indicator_id"] is not None:
                indicators[int(ind["indicator_id"])] = ind

            geo = map_geographic(r)
            if geo["geo_join_id"] is not None:
                geos[int(geo["geo_join_id"])] = geo

        if indicators:
            execute_batch(cur, INSERT_INDICATORS, list(indicators.values()), page_size=batch_size)

        if geos:
            execute_batch(cur, INSERT_GEOGRAPHIC, list(geos.values()), page_size=batch_size)

        # 2) Insert measurements (count real inserts via RETURNING)
        measurements = [map_measurement(r, run_id) for r in valid_records]

        measurements_inserted = 0
        if measurements:
            execute_batch(cur, INSERT_MEASUREMENTS, measurements, page_size=batch_size)
            # RETURNING rows are in the cursor now; duplicates return nothing
            returned = cur.fetchall()
            measurements_inserted = len(returned)

        # 3) Insert rejects with reasons (count via RETURNING)
        reject_rows = []
        for r in rejected_records:
            sanitized = sanitize_for_json(r)
            reason = sanitized.get("error_reason", "Validation failed")
            reject_rows.append(
                (
                    run_id,
                    json.dumps(sanitized, allow_nan=False),
                    reason,
                    source_file,
                )
            )

        rejects_inserted = 0
        if reject_rows:
            execute_batch(cur, INSERT_REJECT, reject_rows, page_size=batch_size)
            rejects_inserted = len(cur.fetchall())

        conn.commit()
        return (measurements_inserted, rejects_inserted)

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()