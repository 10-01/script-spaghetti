"""
Helper functions - some of these overlap with utils.py but we use them
in the compute scripts. TODO: consolidate these someday
"""
import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

# DB connection - yes this is duplicated from utils.py, I know
_DB_HOST = os.environ.get("DB_HOST", "localhost")
_DB_PORT = os.environ.get("DB_PORT", "5432")
_DB_NAME = os.environ.get("DB_NAME", "football_data")
_DB_USER = os.environ.get("DB_USER", "postgres")
_DB_PASS = os.environ.get("DB_PASSWORD", "postgres")


def getDbConnection():
    """Returns a postgres connection. Caller is responsible for closing it."""
    return psycopg2.connect(
        host=_DB_HOST,
        port=_DB_PORT,
        dbname=_DB_NAME,
        user=_DB_USER,
        password=_DB_PASS
    )


def runQuery(query, params=None, fetch=False):
    """Run a query. If fetch=True, returns rows."""
    conn = getDbConnection()
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        if fetch:
            rows = cur.fetchall()
            conn.commit()
            return rows
        else:
            conn.commit()
            return None
    except Exception as ex:
        conn.rollback()
        print("Query failed:", str(ex))
        raise ex
    finally:
        cur.close()
        conn.close()


def truncate_table(table_name):
    """Truncate a computed table before recomputing. Use with caution."""
    # NOTE: we truncate and reinsert because the upsert logic was getting complicated
    # and sarah said this was fine for now since compute runs at night
    runQuery(f"TRUNCATE TABLE {table_name}")


def get_all_leagues():
    """Return league IDs we track."""
    return [39, 140, 135, 253]


def safe_int(val):
    """Convert to int, return 0 if None or invalid."""
    if val is None:
        return 0
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def safe_float(val, decimals=2):
    if val is None:
        return 0.0
    try:
        return round(float(val), decimals)
    except:
        return 0.0
