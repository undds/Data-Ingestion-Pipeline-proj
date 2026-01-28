from typing import List, Dict


def deduplicate_records(records: List[Dict], keys: List[str]) -> List[Dict]:
    seen = set()
    unique_records = []

    for record in records:
        try:
            key = tuple(record[k] for k in keys)
        except KeyError as e:
            raise KeyError(f"Deduplication key '{e.args[0]}' missing in record: {record}")

        if key not in seen:
            seen.add(key)
            unique_records.append(record)

    return unique_records


def count_duplicates(records: List[Dict], keys: List[str]) -> int:
    seen = set()
    duplicates = 0

    for record in records:
        key = tuple(record[k] for k in keys)
        if key in seen:
            duplicates += 1
        else:
            seen.add(key)

    return duplicates