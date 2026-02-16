# pretty sure end_timestamp should be default current timestamp. if the record is being placed into database without timestamp, that means ingestion has been finished and current time is end time at most.

# need to store start timestamp before ingestion starts
CREATE_INGESTION_RUNS = """
CREATE TABLE IF NOT EXISTS ingestion_runs (
    run_id          SERIAL PRIMARY KEY,
    source_file     VARCHAR,
    start_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    end_timestamp   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    total_records   INTEGER,
    valid_records   INTEGER,
    rejected_records INTEGER,
    status          VARCHAR(50),
    error_message   TEXT
);
"""

# imo dont even need this table. 
# if there is a nonzero amount of rejected records in ingestion runs then we know there is a rejection
# error reason can be stored in error_message of ingestion_runs
# rejected_at will be the end timestamp
# keeping for now because it was in the original design
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