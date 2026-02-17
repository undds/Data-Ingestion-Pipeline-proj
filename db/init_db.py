import logging
from db.connection import connect_to_db
from db.schema import CREATE_INGESTION_RUNS, CREATE_INGESTION_REJECTS, CREATE_MEASUREMENTS, CREATE_INDICATORS, CREATE_GEOGRAPHIC

def init_db(reset: bool = True) -> None:
    """
    Initialize database tables.

    reset=False → only CREATE IF NOT EXISTS (safe for production)
    reset=True  → DROP + recreate tables (development only)
    """

    conn = connect_to_db()
    cur = conn.cursor()

    try:
        if reset:
            # Drop child tables first (FK dependencies)
            cur.execute("DROP TABLE IF EXISTS ingestion_rejects;")
            cur.execute("DROP TABLE IF EXISTS measurements;")

            # Drop parent tables after
            cur.execute("DROP TABLE IF EXISTS geographic;")
            cur.execute("DROP TABLE IF EXISTS indicators;")
            cur.execute("DROP TABLE IF EXISTS ingestion_runs;")

        # Create parent tables first
        cur.execute(CREATE_INGESTION_RUNS)
        cur.execute(CREATE_INDICATORS)
        cur.execute(CREATE_GEOGRAPHIC)

        # Then child tables
        cur.execute(CREATE_MEASUREMENTS)
        cur.execute(CREATE_INGESTION_REJECTS)

        conn.commit()
        logging.info("Database tables verified/created successfully")

    except Exception as e:
        conn.rollback()
        logging.error(f"Database initialization failed: {e}")
        raise

    finally:
        cur.close()
        conn.close()