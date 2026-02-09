import logging
from db.connection import connect_to_db
from db.schema import CREATE_INGESTION_RUNS, CREATE_INGESTION_REJECTS, CREATE_MEASUREMENTS, CREATE_INDICATORS, CREATE_GEOGRAPHIC

def init_db():

    conn = connect_to_db()
    cur = conn.cursor()

    try:
        # hmm why drop? i think this means that we want to replace every data with new data. but what if we want to append new data to old data?

        cur.execute("DROP TABLE IF EXISTS ingestion_runs;")
        cur.execute("DROP TABLE IF EXISTS ingestion_rejects;")
        cur.execute("DROP TABLE IF EXISTS measurements;")
        cur.execute("DROP TABLE IF EXISTS indicators;")
        cur.execute("DROP TABLE IF EXISTS geographic;")
        

        # Recreate tables
        cur.execute(CREATE_INGESTION_RUNS)
        cur.execute(CREATE_INGESTION_REJECTS)
        cur.execute(CREATE_MEASUREMENTS)
        cur.execute(CREATE_INDICATORS)
        cur.execute(CREATE_GEOGRAPHIC)

        conn.commit()
        logging.info("Database tables verified/created successfully")

    except Exception as e:
        conn.rollback()
        logging.error(f"Database initialization failed: {e}")
        raise

    finally:
        cur.close()
        conn.close()