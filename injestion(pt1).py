import os
import logging
import sys
from collections import Counter

# Ensure Python finds your modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from config.config_loader import load_config
from db.init_db import init_db
from db.connection import connect_to_db
from ingestion.read import read_csv
from ingestion.validate import validate_records
from ingestion.deduplicator import deduplicate_records
from ingestion.loader import load_records


def setup_logging(log_level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        filename="logs/ingestion.log",
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )
    # Add console handler so you see logs in terminal too
    console = logging.StreamHandler()
    console.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logging.getLogger("").addHandler(console)


def log_reject_summary(rejected_records: list[dict], sample_size: int = 5) -> None:
    """
    Log a summary of rejected records (top reasons) and a small sample.
    """
    if not rejected_records:
        return

    reasons = [r.get("error_reason", "Validation failed") for r in rejected_records]
    counts = Counter(reasons)

    logging.warning(f"Reject summary: {len(rejected_records)} rejected total")
    for reason, cnt in counts.most_common(5):
        logging.warning(f"Reject reason ({cnt}): {reason}")

    for i, r in enumerate(rejected_records[:sample_size], start=1):
        # Safely get keys for logging
        logging.warning(
            f"Reject sample {i}: reason={r.get('error_reason')} "
            f"ID={r.get('Unique ID', 'N/A')} "
            f"Place={r.get('Geo Place Name', 'N/A')}"
        )


def main() -> None:
    # 0) Load config from YAML
    # Uses absolute path calculation to be safe
    base_dir = os.path.dirname(os.path.abspath(__file__))
    config_path = os.path.join(base_dir, "config", "ingestion.yaml")

    cfg = load_config(config_path)


def finish_run(
    run_id: int,
    records_ingested: int,
    records_approved: int,
    records_rejected: int,
    records_deduped: int,
    status: str = "SUCCESS",
    error_message: str | None = None,
) -> None:
    conn = connect_to_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            UPDATE ingestion_runs
            SET end_timestamp = CURRENT_TIMESTAMP,
                records_ingested = %s,
                records_approved = %s,
                records_rejected = %s,
                records_deduped = %s,
                status = %s,
                error_message = %s
            WHERE run_id = %s;
            """,
            (
                records_ingested,
                records_approved,
                records_rejected,
                records_deduped,
                status,
                error_message,
                run_id,
            ),
        )
        conn.commit()
    finally:
        cur.close()
        conn.close()


def main() -> None:
    cfg = load_config("config/ingestion.yaml")
    setup_logging(cfg["app"].get("log_level", "INFO"))
    logging.info("Starting Air Quality Data Ingestion")

    # Create tables
    init_db(reset=False)

    # 2) Read raw data
    src_path = cfg["data_source"]["path"]
    # Handle relative path in config if needed
    if not os.path.isabs(src_path):
        src_path = os.path.join(base_dir, src_path)

    raw_records = read_csv(src_path)
    logging.info(f"Records read: {len(raw_records)}")

    # 3) Validate using YAML rules
    validation_cfg = cfg.get("validation", {})
    required_fields = validation_cfg.get("required_fields", [])
    numeric_fields = validation_cfg.get("numeric_fields", [])
    date_fields = validation_cfg.get("date_fields", [])

    valid_records, rejected_records = validate_records(
        raw_records,
        required_fields=required_fields,
        numeric_fields=numeric_fields,
        date_fields=date_fields,
    )

    logging.info(f"Valid records: {len(valid_records)}")
    logging.info(f"Rejected records: {len(rejected_records)}")

    log_reject_summary(rejected_records, sample_size=5)

    # 4) Deduplicate valid records
    dedup_cfg = cfg.get("deduplication", {})
    if dedup_cfg.get("enabled", True):
        dedup_keys = dedup_cfg.get("keys", [])
        deduped_records = deduplicate_records(valid_records, dedup_keys)
    else:
        deduped_records = valid_records

    logging.info(f"Records after deduplication: {len(deduped_records)}")

    # 5) Load to database
    # UPDATED SECTION: Using the new loader signature
    source_file = os.path.basename(src_path)

    # We map the config values to the new function arguments.
    # New tables (Runs, Indicators, Locations) are hardcoded here
    # or you can add them to your YAML.

    load_records(
        valid_records=deduped_records,
        rejected_records=rejected_records,
        source_file=source_file,
        # New Tables mapping
        ingestion_runs_table="ingestion_runs",
        ingestion_reject_table=cfg["database"].get("reject_table", "stg_rejects"),
        measurements_table=cfg["database"].get("target_table", "stg_air_quality_ny"),
        indicators_table="dim_indicators",
        geographic_table="dim_locations",
        batch_size=cfg["database"].get("batch_size", 500),
    )

    try:
        raw_records = read_csv(src_path)

        # Trigger rejects via YAML (optional)
        force_reject = cfg.get("testing", {}).get("force_reject", False)
        if force_reject and raw_records:
            forced_bad = dict(raw_records[0])
            forced_bad["name"] = None
            raw_records.append(forced_bad)

        logging.info(f"Records read: {len(raw_records)}")

        required_fields = cfg["validation"].get("required_fields", [])
        numeric_fields = cfg["validation"].get("numeric_fields", [])
        date_fields = cfg["validation"].get("date_fields", [])

        valid_records, rejected_records = validate_records(
            raw_records,
            required_fields=required_fields,
            numeric_fields=numeric_fields,
            date_fields=date_fields,
        )

        logging.info(f"Valid records: {len(valid_records)}")
        logging.info(f"Rejected records: {len(rejected_records)}")
        log_reject_summary(rejected_records, sample_size=5)

        dedup_cfg = cfg.get("deduplication", {})
        if dedup_cfg.get("enabled", True):
            dedup_keys = dedup_cfg.get("keys", [])
            deduped_records = deduplicate_records(valid_records, dedup_keys)
        else:
            deduped_records = valid_records

        logging.info(f"Records after deduplication: {len(deduped_records)}")

        # Load (normalized schema)
        load_records(
            valid_records=deduped_records,
            rejected_records=rejected_records,
            source_file=source_file,
            run_id=run_id,
            batch_size=cfg["database"].get("batch_size", 500),
        )

        finish_run(
            run_id=run_id,
            records_ingested=len(raw_records),
            records_approved=len(valid_records),
            records_rejected=len(rejected_records),
            records_deduped=len(deduped_records),
            status="SUCCESS",
            error_message=None,
        )

        logging.info("Air Quality Data Ingestion completed successfully")

    except Exception as e:
        logging.exception(f"Ingestion failed for run_id={run_id}: {e}")
        finish_run(
            run_id=run_id,
            records_ingested=0,
            records_approved=0,
            records_rejected=0,
            records_deduped=0,
            status="FAILED",
            error_message=str(e),
        )
        raise


if __name__ == "__main__":
    main()
