import math
from typing import List, Dict


def clean_records(records: List[Dict]) -> List[Dict]:
    cleaned = []

    for record in records:
        cleaned_record = {}

        for key, value in record.items():
            # Convert Pandas NaN to None
            if isinstance(value, float) and math.isnan(value):
                cleaned_record[key] = None
            else:
                cleaned_record[key] = value

        cleaned.append(cleaned_record)

    return cleaned