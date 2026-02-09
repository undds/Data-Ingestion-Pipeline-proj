CREATE_INGESTION_RUNS = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id          SERIAL PRIMARY KEY,
    source_file     VARCHAR,
    start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_timestamp   TIMESTAMP,
    records_ingested INTEGER,
    records_approved INTEGER,
    records_rejected INTEGER,
    records_deduped INTEGER,
    status          VARCHAR(50),
    error_message   TEXT
);
"""

CREATE_INGESTION_REJECTS = """
CREATE TABLE IF NOT EXISTS ingestion_rejects (
    reject_id       SERIAL PRIMARY KEY,
    run_id          INTEGER,
    raw_record      JSONB,
    error_reason    TEXT,
    source_file     VARCHAR,
    rejected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_MEASUREMENTS = """
CREATE TABLE IF NOT EXISTS measurements (
    unique_id       SERIAL PRIMARY KEY,
    indicator_id    INTEGER,
    geo_join_id     INTEGER,
    time_period     TEXT,
    start_date      DATE,
    data_value      NUMERIC,
    message         TEXT,
    run_id          INTEGER,
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_INDICATORS = """
CREATE TABLE IF NOT EXISTS indicators (
    indicator_id    SERIAL PRIMARY KEY,
    name            TEXT,
    measure         TEXT,
    measure_info    TEXT
);
"""

CREATE_GEOGRAPHIC = """
CREATE TABLE IF NOT EXISTS geographic (
    geo_join_id     TEXT PRIMARY KEY,
    geo_type_name   TEXT,
    geo_place_name  TEXT
);
"""