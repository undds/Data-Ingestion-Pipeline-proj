import pandas as pd
import pytest

from ingestion.read import read_csv


def test_read_csv_returns_list_of_dicts(tmp_path):
    """
    Verify read_csv reads a CSV and returns a list of dictionaries.
    """

    # Create a temporary CSV file
    csv_content = pd.DataFrame(
        {
            "Unique ID": [1, 2],
            "Indicator ID": [101, 102],
            "Geo Place Name": ["New York", "Brooklyn"],
            "Start_Date": ["2020-01-01", "2020-02-01"],
            "Data Value": [12.5, 8.1],
        }
    )

    test_file = tmp_path / "test_air_quality.csv"
    csv_content.to_csv(test_file, index=False)

    records = read_csv(str(test_file))

    assert isinstance(records, list)
    assert len(records) == 2
    assert isinstance(records[0], dict)


def test_read_csv_normalizes_column_names(tmp_path):
    """
    Verify column names are normalized (lowercase, underscores).
    """

    csv_content = pd.DataFrame(
        {
            "Unique ID": [1],
            "Geo Place Name": ["New York"],
        }
    )

    test_file = tmp_path / "test_columns.csv"
    csv_content.to_csv(test_file, index=False)

    records = read_csv(str(test_file))

    assert "unique_id" in records[0]
    assert "geo_place_name" in records[0]


def test_read_csv_file_not_found():
    """
    Verify an exception is raised if the file does not exist.
    """

    with pytest.raises(Exception):
        read_csv("non_existent_file.csv")