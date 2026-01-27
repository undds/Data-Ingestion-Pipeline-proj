import pandas
import logging
from datetime import datetime

# Getting the current date and time
current_time = datetime.now()
# Getting the timestamp
timestamp = current_time.timestamp()
print("Current time:", current_time)
print("Timestamp:", timestamp)



logging.basicConfig(level=logging.INFO)


def read_csv_file(file_path):
    try:
        data = pandas.read_csv(file_path)
        logging.info("CSV file read successfully at %s", datetime.now())
    except ValueError:
        logging.warning("CSV parsing failed at %s", datetime.now())
        data = None
    
    
    return data

def read_json_file(file_path):
    data = pandas.read_json(file_path)
    return data

if __name__ == "__main__":
    csv_data = read_csv_file("data/Air_Quality.csv")
    print("CSV Data:")
    print(csv_data)

    json_data = read_json_file("data/sample.json")
    print("JSON Data:")
    print(json_data)
