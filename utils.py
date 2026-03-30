"""
Utility functions for the football data pipeline.
Originally written by Dave, updated by Sarah for the MLS expansion.
"""
import psycopg2
import config

def get_db_conn():
    """Get a database connection."""
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASS,
    )
    return conn


def execute_query(query, params=None):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        conn.commit()
    except Exception as e:
        conn.rollback()
        print(f"ERROR executing query: {e}")
        raise
    finally:
        cur.close()
        conn.close()


def fetch_all(query, params=None):
    conn = get_db_conn()
    cur = conn.cursor()
    try:
        cur.execute(query, params)
        results = cur.fetchall()
        return results
    except Exception as e:
        print(f"ERROR fetching: {e}")
        return []
    finally:
        cur.close()
        conn.close()


def upsert_fixture(fixture_data):
    """Insert or update a fixture record."""
    query = """
        INSERT INTO raw_fixtures (fixture_id, league_id, league_name, season, round, date,
            venue_name, venue_city, home_team_id, home_team_name, away_team_id, away_team_name,
            home_goals, away_goals, home_goals_ht, away_goals_ht, status_short, status_long,
            referee, raw_json, updated_at)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
        ON CONFLICT (fixture_id)
        DO UPDATE SET
            home_goals = EXCLUDED.home_goals,
            away_goals = EXCLUDED.away_goals,
            home_goals_ht = EXCLUDED.home_goals_ht,
            away_goals_ht = EXCLUDED.away_goals_ht,
            status_short = EXCLUDED.status_short,
            status_long = EXCLUDED.status_long,
            referee = EXCLUDED.referee,
            raw_json = EXCLUDED.raw_json,
            updated_at = NOW()
    """
    execute_query(query, fixture_data)


def get_league_name(league_id):
    """Return human-readable league name."""
    names = {
        39: "Premier League",
        140: "La Liga",
        135: "Serie A",
        253: "MLS",
    }
    return names.get(league_id, f"Unknown ({league_id})")
