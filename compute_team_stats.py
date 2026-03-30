#!/usr/bin/env python3
#
# compute_team_stats.py
#
# Computes team-level stats: home/away records, goals, clean sheets
# from the raw_fixtures table.
#
# This one is pretty straightforward but the SQL is gnarly.
# Don't @ me.
#

import logging
import psycopg2
from datetime import datetime

# using logging here because team stats is the "important" compute job
# and we want proper log levels
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
log = logging.getLogger("compute_team_stats")

# Inline DB config - yes I know we have config.py and helpers.py
# but this script predates both of those
import os
from dotenv import load_dotenv
load_dotenv()

DB_PARAMS = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": os.getenv("DB_PORT", "5432"),
    "dbname": os.getenv("DB_NAME", "football_data"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", "postgres"),
}


def compute_team_stats(league_id, season):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    log.info(f"Computing team stats: league={league_id} season={season}")

    # Get all finished fixtures for this league/season
    cur.execute("""
        SELECT fixture_id, home_team_id, home_team_name, away_team_id, away_team_name,
               home_goals, away_goals, status_short
        FROM raw_fixtures
        WHERE league_id = %s AND season = %s AND status_short = 'FT'
        ORDER BY date
    """, (league_id, season))

    fixtures = cur.fetchall()
    log.info(f"  Found {len(fixtures)} completed fixtures")

    teams = {}

    for fix in fixtures:
        fid, home_id, home_name, away_id, away_name, hg, ag, status = fix

        # handle nulls
        hg = hg or 0
        ag = ag or 0

        # init teams if needed
        for tid, tname in [(home_id, home_name), (away_id, away_name)]:
            if tid not in teams:
                teams[tid] = {
                    "team_id": tid,
                    "team_name": tname,
                    "total_matches": 0,
                    "home_wins": 0, "home_draws": 0, "home_losses": 0,
                    "away_wins": 0, "away_draws": 0, "away_losses": 0,
                    "goals_scored": 0, "goals_conceded": 0,
                    "clean_sheets": 0,
                    "home_goals_scored": 0, "away_goals_scored": 0,
                }

        # home team stats
        teams[home_id]["total_matches"] += 1
        teams[home_id]["goals_scored"] += hg
        teams[home_id]["goals_conceded"] += ag
        teams[home_id]["home_goals_scored"] += hg
        if ag == 0:
            teams[home_id]["clean_sheets"] += 1

        if hg > ag:
            teams[home_id]["home_wins"] += 1
        elif hg == ag:
            teams[home_id]["home_draws"] += 1
        else:
            teams[home_id]["home_losses"] += 1

        # away team stats
        teams[away_id]["total_matches"] += 1
        teams[away_id]["goals_scored"] += ag
        teams[away_id]["goals_conceded"] += hg
        teams[away_id]["away_goals_scored"] += ag
        if hg == 0:
            teams[away_id]["clean_sheets"] += 1

        if ag > hg:
            teams[away_id]["away_wins"] += 1
        elif ag == hg:
            teams[away_id]["away_draws"] += 1
        else:
            teams[away_id]["away_losses"] += 1

    # write to computed_team_stats
    for tid, s in teams.items():
        avg_scored = round(s["goals_scored"] / s["total_matches"], 2) if s["total_matches"] > 0 else 0
        avg_conceded = round(s["goals_conceded"] / s["total_matches"], 2) if s["total_matches"] > 0 else 0

        try:
            cur.execute("""
                INSERT INTO computed_team_stats
                    (team_id, team_name, league_id, season, total_matches,
                     home_wins, home_draws, home_losses,
                     away_wins, away_draws, away_losses,
                     goals_scored, goals_conceded, clean_sheets,
                     avg_goals_scored, avg_goals_conceded,
                     home_goals_scored, away_goals_scored, last_computed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (team_id, league_id, season)
                DO UPDATE SET
                    total_matches = EXCLUDED.total_matches,
                    home_wins = EXCLUDED.home_wins,
                    home_draws = EXCLUDED.home_draws,
                    home_losses = EXCLUDED.home_losses,
                    away_wins = EXCLUDED.away_wins,
                    away_draws = EXCLUDED.away_draws,
                    away_losses = EXCLUDED.away_losses,
                    goals_scored = EXCLUDED.goals_scored,
                    goals_conceded = EXCLUDED.goals_conceded,
                    clean_sheets = EXCLUDED.clean_sheets,
                    avg_goals_scored = EXCLUDED.avg_goals_scored,
                    avg_goals_conceded = EXCLUDED.avg_goals_conceded,
                    home_goals_scored = EXCLUDED.home_goals_scored,
                    away_goals_scored = EXCLUDED.away_goals_scored,
                    last_computed = NOW()
            """, (
                tid, s["team_name"], league_id, season, s["total_matches"],
                s["home_wins"], s["home_draws"], s["home_losses"],
                s["away_wins"], s["away_draws"], s["away_losses"],
                s["goals_scored"], s["goals_conceded"], s["clean_sheets"],
                avg_scored, avg_conceded,
                s["home_goals_scored"], s["away_goals_scored"],
            ))
        except Exception as e:
            log.error(f"Failed to insert team stats for {s['team_name']}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()

    return len(teams)


if __name__ == "__main__":
    log.info("=" * 50)
    log.info("Team Stats Computation Starting")
    log.info("=" * 50)

    # leagues and seasons
    league_seasons = [
        (39, 2025),    # Premier League
        (140, 2025),   # La Liga
        (135, 2025),   # Serie A
        (253, 2026),   # MLS
    ]

    for lid, szn in league_seasons:
        try:
            count = compute_team_stats(lid, szn)
            log.info(f"League {lid}: {count} teams computed")
        except Exception as e:
            log.error(f"FAILED for league {lid}: {e}")

    log.info("Team stats computation done.")
