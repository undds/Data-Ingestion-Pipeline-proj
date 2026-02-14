import os
import logging
from collections import Counter

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

    src_path = cfg["data_source"]["path"]
    source_file = os.path.basename(src_path)

    # Start run tracking
    run_id = start_run(source_file)
    logging.info(f"Run started: run_id={run_id}")

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