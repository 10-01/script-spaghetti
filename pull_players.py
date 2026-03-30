#!/usr/bin/env python3
#
# pull_players.py - pulls player squad data for all teams
#
# This is the slowest script because we need to make one API call per team.
# With 4 leagues x ~20 teams each = ~80 API calls just for this script.
# Free tier is 100/day so be careful running this with other scripts.
#
# 2024-09-20: reduced to only pull teams that we don't already have players for
# 2025-01-15: actually that broke things, reverted to pulling all teams again
#

import requests
import json
import psycopg2
import time
import sys
from datetime import datetime

import config

API_URL = config.API_BASE_URL
HEADERS = config.get_headers()

print(f"[{datetime.now()}] Player pull starting...")
print(f"API URL: {API_URL}")

def get_conn():
    # copy pasted from pull_standings.py, should use utils but whatever
    import os
    from dotenv import load_dotenv
    load_dotenv()
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "football_data"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "postgres")
    )


def get_team_ids_for_league(league_id, season):
    """Get all team IDs for a league from our database."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT DISTINCT team_id FROM raw_standings WHERE league_id = %s AND season = %s",
                (league_id, season))
    team_ids = [row[0] for row in cur.fetchall()]
    cur.close()
    conn.close()
    return team_ids


def pull_squad(team_id):
    """Pull player/squad data for a team."""
    url = f"{API_URL}/players/squads"
    params = {"team": team_id}

    resp = requests.get(url, headers=HEADERS, params=params)

    if resp.status_code != 200:
        print(f"  ERROR getting squad for team {team_id}: {resp.status_code}")
        return []

    data = resp.json()
    response = data.get("response", [])

    if not response:
        return []

    return response[0].get("players", [])


def save_players(team_id, players):
    conn = get_conn()
    cur = conn.cursor()
    saved = 0

    for p in players:
        try:
            cur.execute("""
                INSERT INTO raw_players (player_id, team_id, name, age, nationality, position, photo_url, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (player_id, team_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    age = EXCLUDED.age,
                    position = EXCLUDED.position,
                    photo_url = EXCLUDED.photo_url,
                    raw_json = EXCLUDED.raw_json
            """, (
                p["id"],
                team_id,
                p["name"],
                p.get("age"),
                p.get("nationality"),  # NOTE: squads endpoint doesn't always have this
                p.get("position"),
                p.get("photo"),
                json.dumps(p)
            ))
            saved += 1
        except Exception as e:
            print(f"  Error saving player {p.get('name', '?')}: {e}")
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()
    return saved


if __name__ == "__main__":
    total_players = 0
    total_teams = 0

    for league_name, league_id in config.LEAGUES.items():
        season = config.get_season_for_league(league_id)
        print(f"\n--- {league_name} (season {season}) ---")

        team_ids = get_team_ids_for_league(league_id, season)
        print(f"  Found {len(team_ids)} teams in database")

        if not team_ids:
            print(f"  WARNING: No teams found! Did pull_standings run first?")
            # fall back to API call to get teams
            # this is a hack but it works
            url = f"{API_URL}/teams"
            params = {"league": league_id, "season": season}
            r = requests.get(url, headers=HEADERS, params=params)
            if r.status_code == 200:
                teams_data = r.json().get("response", [])
                team_ids = [t["team"]["id"] for t in teams_data]
                print(f"  Got {len(team_ids)} teams from API instead")

        for tid in team_ids:
            players = pull_squad(tid)
            if players:
                saved = save_players(tid, players)
                print(f"  Team {tid}: {saved} players saved")
                total_players += saved
                total_teams += 1

            # rate limiting - don't hammer the API
            time.sleep(1)

    print(f"\n[{datetime.now()}] Player pull complete.")
    print(f"Teams processed: {total_teams}")
    print(f"Players saved: {total_players}")
