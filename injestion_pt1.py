import os
import logging
from collections import Counter

from config.config_loader import load_config
from db.init_db import init_db
from db.connection import connect_to_db
from ingestion.read import read_csv
from ingestion.validate import validate_records
from ingestion.loader import load_records


def setup_logging(log_level: str = "INFO") -> None:
    os.makedirs("logs", exist_ok=True)
    logging.basicConfig(
        filename="logs/ingestion.log",
        level=getattr(logging, log_level.upper(), logging.INFO),
        format="%(asctime)s - %(levelname)s - %(message)s",
    )


def log_reject_summary(rejected_records: list[dict], sample_size: int = 5) -> None:
    if not rejected_records:
        return

    reasons = [r.get("error_reason", "Validation failed") for r in rejected_records]
    counts = Counter(reasons)

    logging.warning(f"Reject summary: {len(rejected_records)} rejected total")
    for reason, cnt in counts.most_common(5):
        logging.warning(f"Reject reason ({cnt}): {reason}")

    for i, r in enumerate(rejected_records[:sample_size], start=1):
        logging.warning(
            f"Reject sample {i}: reason={r.get('error_reason')} "
            f"unique_id={r.get('unique_id')} indicator_id={r.get('indicator_id')} "
            f"geo_place_name={r.get('geo_place_name')} start_date={r.get('start_date')}"
        )


def start_run(source_file: str) -> int:
    conn = connect_to_db()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO ingestion_runs (source_file, status)
            VALUES (%s, 'STARTED')
            RETURNING run_id;
            """,
            (source_file,),
        )
        run_id = cur.fetchone()[0]
        conn.commit()
        return run_id
    finally:
        cur.close()
        conn.close()


def finish_run(
    run_id: int,
    total_records: int,
    valid_records: int,
    rejected_records: int,
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
                total_records = %s,
                valid_records = %s,
                rejected_records = %s,
                status = %s,
                error_message = %s
            WHERE run_id = %s;
            """,
            (
                total_records,
                valid_records,
                rejected_records,
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

    src_path = cfg["data_source"]["path"]
    source_file = os.path.basename(src_path)

    # Start run tracking
    run_id = start_run(source_file)
    logging.info(f"Run started: run_id={run_id}, source_file={source_file}")

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

        valid_records = [r for r in valid_records if r.get("unique_id") is not None]

        # Load (normalized schema)
        load_records(
            valid_records=valid_records,
            rejected_records=rejected_records,
            source_file=source_file,
            batch_size=cfg["database"].get("batch_size", 500),
        )

        finish_run(
            run_id=run_id,
            total_records=len(valid_records) + len(rejected_records),
            valid_records=len(valid_records),
            rejected_records=len(rejected_records),
            status="SUCCESS",
            error_message=None,
        )

        logging.info("Air Quality Data Ingestion completed successfully")

    except Exception as e:
        logging.exception(f"Ingestion failed for run_id={run_id}: {e}")
        finish_run(
            run_id=run_id,
            valid_records=0,
            rejected_records=len(rejected_records),
            total_records=len(raw_records),
            status="FAILED",
            error_message=str(e),
        )
        raise


if __name__ == "__main__":
    main()
