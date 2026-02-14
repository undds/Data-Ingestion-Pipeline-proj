CREATE_INGESTION_RUNS = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id              SERIAL PRIMARY KEY,
    source_file         VARCHAR NOT NULL,
    start_timestamp     TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_timestamp       TIMESTAMP,
    records_ingested    INTEGER DEFAULT 0,
    records_approved    INTEGER DEFAULT 0,
    records_rejected    INTEGER DEFAULT 0,
    records_deduped     INTEGER DEFAULT 0,
    status              VARCHAR(50) NOT NULL DEFAULT 'STARTED',
    error_message       TEXT
);
"""

CREATE_INDICATORS = """
CREATE TABLE IF NOT EXISTS indicators (
    indicator_id    INTEGER PRIMARY KEY,
    name            TEXT,
    measure         TEXT,
    measure_info    TEXT
);
"""

CREATE_GEOGRAPHIC = """
CREATE TABLE IF NOT EXISTS geographic (
    geo_join_id     INTEGER PRIMARY KEY,
    geo_type_name   TEXT,
    geo_place_name  TEXT
);
"""

CREATE_MEASUREMENTS = """
CREATE TABLE IF NOT EXISTS measurements (
    unique_id       INTEGER PRIMARY KEY,
    indicator_id    INTEGER REFERENCES indicators(indicator_id),
    geo_join_id     INTEGER REFERENCES geographic(geo_join_id),
    time_period     TEXT,
    start_date      DATE,
    data_value      NUMERIC,
    message         TEXT,
    run_id          INTEGER REFERENCES ingestion_runs(run_id),
    load_timestamp  TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""

CREATE_INGESTION_REJECTS = """
CREATE TABLE IF NOT EXISTS ingestion_rejects (
    reject_id       SERIAL PRIMARY KEY,
    run_id          INTEGER REFERENCES ingestion_runs(run_id),
    raw_record      JSONB NOT NULL,
    error_reason    TEXT NOT NULL,
    source_file     VARCHAR NOT NULL,
    rejected_at     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
"""