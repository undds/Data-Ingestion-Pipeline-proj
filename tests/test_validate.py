
from ingestion.validate import validate_records


def test_validate_records_all_valid():
    records = [
        {
            "unique_id": 1,
            "indicator_id": 101,
            "name": "PM2.5",
            "geo_type_name": "City",
            "geo_place_name": "New York",
            "start_date": "2020-01-01",
            "data_value": 12.5,
        },
        {
            "unique_id": 2,
            "indicator_id": 102,
            "name": "PM10",
            "geo_type_name": "Borough",
            "geo_place_name": "Brooklyn",
            "start_date": "2020-02-01",
            "data_value": 8.1,
        },
    ]
    valid, rejected = validate_records(records)

    assert len(valid) == 2
    assert len(rejected) == 0


def test_validate_records_missing_required_field():
    records = [
        {
            "unique_id": 1,
            "indicator_id": 101,
            "geo_place_name": "New York",
            # start_date missing
            "data_value": 12.5,
        }
    ]

    valid, rejected = validate_records(records)

    assert len(valid) == 0
    assert len(rejected) == 1


def test_validate_records_nan_value_rejected():
    records = [
        {
            "unique_id": 1,
            "indicator_id": 101,
            "geo_place_name": "New York",
            "start_date": "2020-01-01",
            "data_value": float("nan"),
        }
    ]

    valid, rejected = validate_records(records)

    assert len(valid) == 0
    assert len(rejected) == 1


def test_validate_records_empty_input():
    records = []

    valid, rejected = validate_records(records)

    assert valid == []
    assert rejected == []