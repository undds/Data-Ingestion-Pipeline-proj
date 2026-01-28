
CREATE_STG_AIR_QUALITY_NY = """
CREATE TABLE IF NOT EXISTS stg_air_quality_ny (
    record_id       SERIAL PRIMARY KEY,
    unique_id       INTEGER,
    indicator_id    INTEGER,
    name            TEXT,
    measure         TEXT,
    measure_info    TEXT,
    geo_type_name   TEXT,
    geo_join_id     TEXT,
    geo_place_name  TEXT,
    time_period     TEXT,
    start_date      DATE,
    data_value      NUMERIC,
    message         TEXT,
    source_file     VARCHAR,
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_STG_REJECTS = """
CREATE TABLE IF NOT EXISTS stg_rejects (
    raw_record     JSONB,
    error_reason   TEXT,
    source_file    VARCHAR,
    rejected_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""