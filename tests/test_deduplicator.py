import pytest

from ingestion.deduplicator import deduplicate_records
from ingestion.deduplicator import count_duplicates

def test_count_duplicates_counts_correctly():
    records = [
        {"unique_id": 1, "indicator_id": 10, "geo_place_name": "NY", "start_date": "2020-01-01"},
        {"unique_id": 1, "indicator_id": 10, "geo_place_name": "NY", "start_date": "2020-01-01"},  
        {"unique_id": 2, "indicator_id": 10, "geo_place_name": "NY", "start_date": "2020-01-01"},
        {"unique_id": 2, "indicator_id": 10, "geo_place_name": "NY", "start_date": "2020-01-01"},  
    ]
    keys = ["unique_id", "indicator_id", "geo_place_name", "start_date"]

    assert count_duplicates(records, keys) == 2


def test_count_duplicates_raises_keyerror_when_key_missing():
    records = [
        {"unique_id": 1, "indicator_id": 10, "start_date": "2020-01-01"} 
    ]
    keys = ["unique_id", "indicator_id", "geo_place_name", "start_date"]

    with pytest.raises(KeyError):
        count_duplicates(records, keys)

def test_deduplicate_removes_exact_duplicates():
    records = [
        {
            "unique_id": 1,
            "indicator_id": 10,
            "geo_place_name": "New York",
            "start_date": "2020-01-01",
            "data_value": 12.3,
        },
        {
            "unique_id": 1,
            "indicator_id": 10,
            "geo_place_name": "New York",
            "start_date": "2020-01-01",
            "data_value": 12.3,
        },
    ]

    dedup_keys = [
        "unique_id",
        "indicator_id",
        "geo_place_name",
        "start_date",
    ]

    result = deduplicate_records(records, dedup_keys)

    assert len(result) == 1
    assert result[0]["unique_id"] == 1


def test_deduplicate_keeps_distinct_records():
    records = [
        {
            "unique_id": 1,
            "indicator_id": 10,
            "geo_place_name": "New York",
            "start_date": "2020-01-01",
        },
        {
            "unique_id": 2,
            "indicator_id": 10,
            "geo_place_name": "New York",
            "start_date": "2020-01-01",
        },
    ]

    dedup_keys = [
        "unique_id",
        "indicator_id",
        "geo_place_name",
        "start_date",
    ]

    result = deduplicate_records(records, dedup_keys)

    assert len(result) == 2


def test_deduplicate_empty_input():
    records = []

    dedup_keys = [
        "unique_id",
        "indicator_id",
        "geo_place_name",
        "start_date",
    ]

    result = deduplicate_records(records, dedup_keys)

    assert result == []


def test_deduplicate_missing_key_raises_error():
    records = [
        {
            "unique_id": 1,
            "indicator_id": 10,
            # geo_place_name missing
            "start_date": "2020-01-01",
        }
    ]

    dedup_keys = [
        "unique_id",
        "indicator_id",
        "geo_place_name",
        "start_date",
    ]

    with pytest.raises(KeyError):
        deduplicate_records(records, dedup_keys)