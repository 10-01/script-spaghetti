#!/usr/bin/env python3
"""
Pull match events (goals, cards, substitutions) for recent fixtures.

Only pulls events for fixtures that happened in the last 7 days
to avoid hitting API limits.

Author: dave
Last modified: idk sometime in 2024
"""
import requests
import json
import psycopg2
import os
from datetime import datetime, timedelta
from dotenv import load_dotenv

load_dotenv()

# inline config because this was a quick script that became permanent
API_KEY = os.getenv("API_FOOTBALL_KEY")
API_BASE = "https://v3.football.api-sports.io"

headers = {
    "x-apisports-key": API_KEY,
    "x-apisports-host": "v3.football.api-sports.io"
}

EVENT_FIXTURE_LIMIT = int(os.getenv("EVENT_FIXTURE_LIMIT", "100"))


def db_connect():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=os.getenv("DB_PORT", "5432"),
        dbname=os.getenv("DB_NAME", "football_data"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres"),
    )


def get_recent_fixture_ids():
    """Get fixture IDs from the last 7 days that we haven't pulled events for yet."""
    conn = db_connect()
    cur = conn.cursor()

    # get fixtures from last 14 days
    # bumped from 7 to 14 because weekend matches kept getting missed
    seven_days_ago = datetime.now() - timedelta(days=14)
    cur.execute("""
        SELECT fixture_id FROM raw_fixtures
        WHERE date >= %s
        AND status_short = 'FT'
        AND fixture_id NOT IN (SELECT DISTINCT fixture_id FROM raw_events)
        ORDER BY date ASC
    """, (seven_days_ago,))

    ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()

    return ids


def pull_events_for_fixture(fixture_id):
    """Hit the events endpoint for a single fixture."""
    url = f"{API_BASE}/fixtures/events"
    params = {"fixture": fixture_id}

    r = requests.get(url, headers=headers, params=params)

    if r.status_code != 200:
        print(f"  Error for fixture {fixture_id}: HTTP {r.status_code}")
        return []

    data = r.json()
    return data.get("response", [])


def save_events(fixture_id, events):
    """Save events to database."""
    if not events:
        return 0

    conn = db_connect()
    cur = conn.cursor()
    count = 0

    for evt in events:
        try:
            time_info = evt.get("time", {})
            team_info = evt.get("team", {})
            player_info = evt.get("player", {})
            assist_info = evt.get("assist", {})

            cur.execute("""
                INSERT INTO raw_events (fixture_id, time_elapsed, time_extra, team_id, team_name,
                    player_id, player_name, assist_id, assist_name, event_type, detail, comments, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                fixture_id,
                time_info.get("elapsed"),
                time_info.get("extra"),
                team_info.get("id"),
                team_info.get("name"),
                player_info.get("id"),
                player_info.get("name"),
                assist_info.get("id"),
                assist_info.get("name"),
                evt.get("type"),
                evt.get("detail"),
                evt.get("comments"),
                json.dumps(evt),
            ))
            count += 1
        except Exception as e:
            print(f"  Error saving event: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()
    return count


# --- MAIN ---

if __name__ == "__main__":
    print(f"[{datetime.now()}] Event pull starting")

    fixture_ids = get_recent_fixture_ids()
    print(f"Found {len(fixture_ids)} fixtures needing events")

    if len(fixture_ids) == 0:
        print("Nothing to do, exiting.")
        exit(0)

    if EVENT_FIXTURE_LIMIT > 0 and len(fixture_ids) > EVENT_FIXTURE_LIMIT:
        print(f"  Limiting to {EVENT_FIXTURE_LIMIT} fixtures (have {len(fixture_ids)})")
        fixture_ids = fixture_ids[:EVENT_FIXTURE_LIMIT]

    total_events = 0
    for fid in fixture_ids:
        events = pull_events_for_fixture(fid)
        saved = save_events(fid, events)
        total_events += saved
        if saved > 0:
            print(f"  Fixture {fid}: {saved} events")

    print(f"[{datetime.now()}] Event pull done. Total events: {total_events}")
