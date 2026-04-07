#!/usr/bin/env python3
"""
Standings puller - gets current league tables
runs after fixtures pull (2:15 AM)
"""
import requests
import json
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# NOTE: this script doesn't use config.py because it was written before we had that
# and nobody bothered to refactor it
API_KEY = os.environ.get("API_FOOTBALL_KEY")
BASE_URL = "https://v3.football.api-sports.io"

HEADERS = {
    "x-apisports-key": API_KEY,
    "x-apisports-host": "v3.football.api-sports.io"
}

# hardcoded league IDs - should match config.py but sometimes they get out of sync
LEAGUE_IDS = [39, 140, 135, 253]

# european season
SEASON = 2025
MLS_SEASON = 2026


def get_conn():
    return psycopg2.connect(
        host=os.environ.get("DB_HOST", "localhost"),
        port=os.environ.get("DB_PORT", "5432"),
        dbname=os.environ.get("DB_NAME", "football_data"),
        user=os.environ.get("DB_USER", "postgres"),
        password=os.environ.get("DB_PASSWORD", "postgres")
    )


def pull_standings(league_id):
    season = MLS_SEASON if league_id == 253 else SEASON

    url = f"{BASE_URL}/standings"
    params = {"league": league_id, "season": season}

    print(f"[{datetime.now()}] Pulling standings for league {league_id}, season {season}")

    r = requests.get(url, headers=HEADERS, params=params)

    if r.status_code != 200:
        print(f"ERROR: got {r.status_code} from API")
        return

    data = r.json()
    if not data.get("response"):
        print(f"No data in response for league {league_id}")
        return

    standings_list = data["response"][0]["league"]["standings"]

    # Some leagues return multiple grouped tables. Flatten them so we persist
    # every team instead of silently dropping later groups/conferences.
    if standings_list and isinstance(standings_list[0], list):
        standings = [team for group in standings_list for team in group]
    else:
        standings = standings_list

    conn = get_conn()
    cur = conn.cursor()

    for team_standing in standings:
        team = team_standing["team"]
        all_stats = team_standing["all"]

        try:
            cur.execute("""
                INSERT INTO raw_standings (league_id, season, team_id, team_name, rank, points,
                    games_played, wins, draws, losses, goals_for, goals_against, goal_diff,
                    form, description, raw_json, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (league_id, season, team_id)
                DO UPDATE SET
                    rank = EXCLUDED.rank,
                    points = EXCLUDED.points,
                    games_played = EXCLUDED.games_played,
                    wins = EXCLUDED.wins,
                    draws = EXCLUDED.draws,
                    losses = EXCLUDED.losses,
                    goals_for = EXCLUDED.goals_for,
                    goals_against = EXCLUDED.goals_against,
                    goal_diff = EXCLUDED.goal_diff,
                    form = EXCLUDED.form,
                    description = EXCLUDED.description,
                    raw_json = EXCLUDED.raw_json,
                    updated_at = NOW()
            """, (
                league_id,
                season,
                team["id"],
                team["name"],
                team_standing["rank"],
                team_standing["points"],
                all_stats["played"],
                all_stats["win"],
                all_stats["draw"],
                all_stats["lose"],
                all_stats["goals"]["for"],
                all_stats["goals"]["against"],
                team_standing["goalsDiff"],
                team_standing.get("form", ""),
                team_standing.get("description", ""),
                json.dumps(team_standing),
            ))
        except Exception as e:
            print(f"  Error inserting standings for team {team['name']}: {e}")
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()
    print(f"  Done - {len(standings)} teams updated")


if __name__ == "__main__":
    print(f"{'='*50}")
    print(f"Standings Pull - {datetime.now()}")
    print(f"{'='*50}")

    for lid in LEAGUE_IDS:
        try:
            pull_standings(lid)
        except Exception as e:
            print(f"FAILED for league {lid}: {e}")
            # keep going, don't let one league failure stop the others

    print("All done.")
