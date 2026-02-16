import os
import logging
import sys
from collections import Counter

# Ensure Python finds your modules
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

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

    setup_logging(cfg["app"].get("log_level", "INFO"))
    logging.info("Starting Air Quality Data Ingestion")

    # 1) Ensure tables exist
    init_db()

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

    logging.info("Air Quality Data Ingestion completed successfully")


if __name__ == "__main__":
    main()
