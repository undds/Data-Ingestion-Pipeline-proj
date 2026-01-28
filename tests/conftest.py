
import pytest
from datetime import datetime

@pytest.fixture
def raw_air_quality_record():
    return {
        "unique_id": 101,
        "indicator_id": "PM2.5",
        "indicator_name": "Fine particles",
        "measure": "Mean",
        "measure_info": "µg/m³",
        "geo_type_name": "City",
        "geo_join_id": "3651000",
        "geo_place_name": "New York",
        "time_period": "2023",
        "start_date": "2023-01-01",
        "data_value": "12.5",
        "message": None
    }


@pytest.fixture
def invalid_air_quality_record():
    return {
        "indicator_id": "",
        "geo_place_name": "",
        "start_date": "bad-date",
        "data_value": "abc"
    }