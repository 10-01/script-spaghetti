#!/usr/bin/env python3
"""
pull team data from api-football
this one doesn't need to run as often, maybe weekly
but its in the daily cron because why not

last modified: 2024-11-02
"""

import requests
import json
import logging
from datetime import datetime
import config
from utils import get_db_conn

# this script uses logging instead of print because sarah said we should
# but the other scripts still use print, oh well
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def fetch_teams_for_league(league_id, season):
    """Get all teams in a league for the given season."""
    url = f"{config.API_BASE_URL}/teams"
    params = {"league": league_id, "season": season}

    logger.info(f"Fetching teams for league {league_id} season {season}")

    response = requests.get(url, headers=config.get_headers(), params=params)

    if response.status_code != 200:
        logger.error(f"API error: {response.status_code} - {response.text[:100]}")
        return []

    data = response.json()
    return data.get("response", [])


def save_team(conn, team_data):
    """Save a single team to the database."""
    team = team_data["team"]
    venue = team_data.get("venue", {}) or {}

    cur = conn.cursor()
    try:
        cur.execute("""
            INSERT INTO raw_teams (team_id, name, code, country, founded, logo_url,
                venue_name, venue_capacity, venue_city, raw_json)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (team_id) DO UPDATE SET
                name = EXCLUDED.name,
                code = EXCLUDED.code,
                logo_url = EXCLUDED.logo_url,
                venue_name = EXCLUDED.venue_name,
                venue_capacity = EXCLUDED.venue_capacity,
                venue_city = EXCLUDED.venue_city,
                raw_json = EXCLUDED.raw_json
        """, (
            team["id"],
            team["name"],
            team.get("code"),
            team.get("country"),
            team.get("founded"),
            team.get("logo"),
            venue.get("name"),
            venue.get("capacity"),
            venue.get("city"),
            json.dumps(team_data)
        ))
        conn.commit()
    except Exception as e:
        conn.rollback()
        logger.error(f"Failed to save team {team['name']}: {e}")
    finally:
        cur.close()


def main():
    logger.info("=" * 40)
    logger.info("Starting team data pull")
    logger.info("=" * 40)

    conn = get_db_conn()
    total = 0

    for league_name, league_id in config.LEAGUES.items():
        season = config.get_season_for_league(league_id)
        teams = fetch_teams_for_league(league_id, season)
        logger.info(f"Got {len(teams)} teams for {league_name}")

        for t in teams:
            save_team(conn, t)
            total += 1

    conn.close()
    logger.info(f"Done. Total teams saved: {total}")


if __name__ == "__main__":
    main()
