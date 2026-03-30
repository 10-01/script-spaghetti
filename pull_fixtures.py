#!/usr/bin/env python3
"""
Pull fixture/match data from API Football.
Runs daily at 2:00 AM ET via cron.
"""
import requests
import json
import psycopg2
from datetime import datetime
import config
from utils import get_db_conn, upsert_fixture, get_league_name

print(f"[{datetime.now()}] Starting fixture pull...")

API_URL = config.API_BASE_URL
HEADERS = config.get_headers()

def pull_fixtures_for_league(league_id, season):
    """Pull all fixtures for a league/season from the API."""
    url = f"{API_URL}/fixtures"
    params = {
        "league": league_id,
        "season": season,
    }

    print(f"  Pulling fixtures for {get_league_name(league_id)} ({season})...")

    resp = requests.get(url, headers=HEADERS, params=params)
    if resp.status_code != 200:
        print(f"  ERROR: API returned {resp.status_code}")
        print(f"  Response: {resp.text[:200]}")
        return 0

    data = resp.json()

    if data.get("errors"):
        print(f"  API Error: {data['errors']}")
        return 0

    fixtures = data.get("response", [])
    print(f"  Got {len(fixtures)} fixtures")

    count = 0
    for fix in fixtures:
        try:
            fixture_info = fix["fixture"]
            league_info = fix["league"]
            teams = fix["teams"]
            goals = fix["goals"]
            score = fix["score"]

            fixture_data = (
                fixture_info["id"],
                league_id,
                league_info["name"],
                season,
                league_info.get("round", ""),
                fixture_info.get("date"),
                fixture_info.get("venue", {}).get("name"),
                fixture_info.get("venue", {}).get("city"),
                teams["home"]["id"],
                teams["home"]["name"],
                teams["away"]["id"],
                teams["away"]["name"],
                goals.get("home"),
                goals.get("away"),
                score.get("halftime", {}).get("home"),
                score.get("halftime", {}).get("away"),
                fixture_info["status"]["short"],
                fixture_info["status"]["long"],
                fixture_info.get("referee"),
                json.dumps(fix),
            )

            upsert_fixture(fixture_data)
            count += 1
        except Exception as e:
            print(f"  Error processing fixture {fix.get('fixture', {}).get('id', '?')}: {e}")
            continue

    return count


# Main
if __name__ == "__main__":
    total = 0
    for league_name, league_id in config.LEAGUES.items():
        season = config.get_season_for_league(league_id)
        pulled = pull_fixtures_for_league(league_id, season)
        total += pulled

    print(f"[{datetime.now()}] Done. Total fixtures processed: {total}")
