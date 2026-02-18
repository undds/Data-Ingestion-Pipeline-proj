import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

DB_PASSWORD = os.getenv("DB_PASSWORD")

def connect_to_db():
    try:
        conn = psycopg2.connect(
            host="127.0.0.1",
            port=5433,
            database="postgres",
            user="postgres",
            password=DB_PASSWORD,
            options="-c lock_timeout=5000",
        )
        return conn
    except Exception as e:
        print(f"Database error: {e}")
        raise