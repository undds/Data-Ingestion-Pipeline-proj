import os
import logging
from collections import Counter

from config.config_loader import load_config
from db.init_db import init_db
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
    """
    Log a summary of rejected records (top reasons) and a small sample.
    Avoids log spam while still making rejects reviewable in ingestion.log.
    """
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


def main() -> None:
    # 0) Load config from YAML
    cfg = load_config("config/ingestion.yaml")

    setup_logging(cfg["app"].get("log_level", "INFO"))
    logging.info("Starting Air Quality Data Ingestion")

    # 1) Ensure tables exist
    init_db()

    # 2) Read raw data (from YAML)
    src_path = cfg["data_source"]["path"]
    raw_records = read_csv(src_path)

    # TEST: force one reject 
    force_reject = False
    if force_reject and raw_records:
        forced_bad = dict(raw_records[0])  
        forced_bad["name"] = None          
        raw_records.append(forced_bad)

    logging.info(f"Records read: {len(raw_records)}")

    # 3) Validate using YAML rules
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

    # 4) Deduplicate valid records only (from YAML)
    dedup_cfg = cfg.get("deduplication", {})
    if dedup_cfg.get("enabled", True):
        dedup_keys = dedup_cfg.get("keys", [])
        deduped_records = deduplicate_records(valid_records, dedup_keys)
    else:
        deduped_records = valid_records

    logging.info(f"Records after deduplication: {len(deduped_records)}")

    # 5) Load to database 
    source_file = os.path.basename(src_path)

    load_records(
        valid_records=deduped_records,
        rejected_records=rejected_records,
        source_file=source_file,
        target_table=cfg["database"]["target_table"],
        reject_table=cfg["database"]["reject_table"],
        batch_size=cfg["database"].get("batch_size", 500),
    )

    logging.info("Air Quality Data Ingestion completed successfully")


if __name__ == "__main__":
    main()