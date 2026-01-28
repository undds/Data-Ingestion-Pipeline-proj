import logging
from db.connection import get_db_connection
from db.schema import CREATE_STG_AIR_QUALITY_NY, CREATE_STG_REJECTS


def init_db():
    conn = get_db_connection()
    cur = conn.cursor()

    try:
        cur.execute("DROP TABLE IF EXISTS stg_air_quality_ny;")
        cur.execute("DROP TABLE IF EXISTS stg_rejects;")

        # Recreate tables
        cur.execute(CREATE_STG_AIR_QUALITY_NY)
        cur.execute(CREATE_STG_REJECTS)

        conn.commit()
        logging.info("Database tables verified/created successfully")

    except Exception as e:
        conn.rollback()
        logging.error(f"Database initialization failed: {e}")
        raise

    finally:
        cur.close()
        conn.close()