from typing import Dict, List, Tuple, Optional
import pandas as pd
import math


DEFAULT_REQUIRED_FIELDS = [
    "unique_id",
    "indicator_id",
    "name",
    "geo_type_name",
    "geo_place_name",
    "start_date",
]

DEFAULT_NUMERIC_FIELDS = ["data_value"]
DEFAULT_DATE_FIELDS = ["start_date"]


def is_nan(value) -> bool:
    return isinstance(value, float) and math.isnan(value)


def clean_value(value):
    """Normalize values for DB insertion."""
    if is_nan(value):
        return None
    if isinstance(value, str):
        value = value.strip()
        return value if value else None
    return value


def validate_record(
    record: Dict,
    required_fields: Optional[List[str]] = None,
    numeric_fields: Optional[List[str]] = None,
    date_fields: Optional[List[str]] = None,
) -> Tuple[bool, Optional[str], Dict]:
    """
    Validate a single record.

    Returns:
        (is_valid, error_reason, cleaned_record)
    """
    req = required_fields or DEFAULT_REQUIRED_FIELDS
    nums = numeric_fields or DEFAULT_NUMERIC_FIELDS
    dates = date_fields or DEFAULT_DATE_FIELDS

    cleaned = {k: clean_value(v) for k, v in record.items()}

    # Required fields
    for field in req:
        if cleaned.get(field) is None:
            return False, f"Missing required field: {field}", cleaned

    # Integer checks
    try:
        cleaned["unique_id"] = int(cleaned["unique_id"])
        cleaned["indicator_id"] = int(cleaned["indicator_id"])
    except (ValueError, TypeError):
        return False, "Invalid integer field", cleaned

    # Numeric checks
    for field in nums:
        if cleaned.get(field) is not None:
            try:
                cleaned[field] = float(cleaned[field])
            except (ValueError, TypeError):
                return False, f"Invalid numeric value for {field}", cleaned

    # Date checks
    for field in dates:
        if cleaned.get(field) is not None:
            try:
                cleaned[field] = pd.to_datetime(cleaned[field]).date()
            except Exception:
                return False, f"Invalid date format for {field}", cleaned

    return True, None, cleaned


def validate_records(
    records: List[Dict],
    required_fields: Optional[List[str]] = None,
    numeric_fields: Optional[List[str]] = None,
    date_fields: Optional[List[str]] = None,
) -> Tuple[List[Dict], List[Dict]]:
    """
    Validate a list of records.

    Returns:
        valid_records: cleaned records that passed validation
        rejected_records: cleaned records with `error_reason`
    """
    valid_records: List[Dict] = []
    rejected_records: List[Dict] = []

    for record in records:
        is_valid, error_reason, cleaned = validate_record(
            record,
            required_fields=required_fields,
            numeric_fields=numeric_fields,
            date_fields=date_fields,
        )

        if is_valid:
            valid_records.append(cleaned)
        else:
            cleaned["error_reason"] = error_reason
            rejected_records.append(cleaned)

    return valid_records, rejected_records