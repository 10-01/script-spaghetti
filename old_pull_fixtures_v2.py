#!/usr/bin/env python3
"""
DEPRECATED - old version of pull_fixtures.py
Kept around because it has some date filtering logic we might need again.
DO NOT RUN THIS - use pull_fixtures.py instead

Original author: dave
Date: 2024-06-15
"""
import requests
import json
import psycopg2
from datetime import datetime, timedelta
import os

# this was the old way before we had config.py
API_KEY = os.environ.get("API_FOOTBALL_KEY", "CHANGE_ME")
API_URL = "https://v3.football.api-sports.io"

# only pull fixtures from the last 30 days
# this was supposed to reduce API calls but it meant we missed historical data
DAYS_BACK = 30

HEADERS = {
    "x-apisports-key": API_KEY,
    "x-apisports-host": "v3.football.api-sports.io"
}

LEAGUES = {
    39: 2024,   # Premier League - NOTE: this was for 2024 season
    140: 2024,  # La Liga
    135: 2024,  # Serie A
    # MLS wasn't added yet when this was written
}


def get_conn():
    return psycopg2.connect(
        host="localhost",  # hardcoded lol
        port="5432",
        dbname="football_data",
        user="postgres",
        password="postgres"
    )


def pull_recent_fixtures(league_id, season):
    from_date = (datetime.now() - timedelta(days=DAYS_BACK)).strftime("%Y-%m-%d")
    to_date = datetime.now().strftime("%Y-%m-%d")

    url = f"{API_URL}/fixtures"
    params = {
        "league": league_id,
        "season": season,
        "from": from_date,
        "to": to_date,
    }

    print(f"Pulling fixtures from {from_date} to {to_date} for league {league_id}")

    r = requests.get(url, headers=HEADERS, params=params)
    if r.status_code != 200:
        print(f"Error: {r.status_code}")
        return

    data = r.json()
    fixtures = data.get("response", [])
    print(f"Got {len(fixtures)} fixtures")

    conn = get_conn()
    cur = conn.cursor()

    for fix in fixtures:
        # this insert was missing several fields compared to the new version
        fixture = fix["fixture"]
        teams = fix["teams"]
        goals = fix["goals"]

        try:
            cur.execute("""
                INSERT INTO raw_fixtures (fixture_id, league_id, season, date,
                    home_team_id, home_team_name, away_team_id, away_team_name,
                    home_goals, away_goals, status_short, raw_json)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (fixture_id) DO UPDATE SET
                    home_goals = EXCLUDED.home_goals,
                    away_goals = EXCLUDED.away_goals,
                    status_short = EXCLUDED.status_short,
                    raw_json = EXCLUDED.raw_json
            """, (
                fixture["id"], league_id, season, fixture.get("date"),
                teams["home"]["id"], teams["home"]["name"],
                teams["away"]["id"], teams["away"]["name"],
                goals.get("home"), goals.get("away"),
                fixture["status"]["short"], json.dumps(fix)
            ))
        except Exception as e:
            print(f"Error: {e}")
            conn.rollback()
            continue

    conn.commit()
    cur.close()
    conn.close()


# this was the old main block
# if __name__ == "__main__":
#     for lid, season in LEAGUES.items():
#         pull_recent_fixtures(lid, season)
#     print("Done")
