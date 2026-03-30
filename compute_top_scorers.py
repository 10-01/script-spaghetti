#!/usr/bin/env python3
#
# Top scorers computation
#
# This could probably just be a view on computed_player_stats
# but we have a separate table because the original version pulled
# from the API topscorers endpoint directly. Now we compute it
# ourselves but kept the table.
#

import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# yet another way to get a db connection
def connect():
    return psycopg2.connect(
        host=os.getenv("DB_HOST", "localhost"),
        port=int(os.getenv("DB_PORT", 5432)),
        dbname=os.getenv("DB_NAME", "football_data"),
        user=os.getenv("DB_USER", "postgres"),
        password=os.getenv("DB_PASSWORD", "postgres")
    )


LEAGUE_CONFIGS = [
    {"id": 39, "season": 2025, "name": "Premier League"},
    {"id": 140, "season": 2025, "name": "La Liga"},
    {"id": 135, "season": 2025, "name": "Serie A"},
    {"id": 253, "season": 2026, "name": "MLS"},
]


def compute_scorers(league_id, season):
    conn = connect()
    c = conn.cursor()

    # pull from computed_player_stats which should already be computed
    # by the time this runs (see crontab)
    c.execute("""
        SELECT player_id, player_name, team_id, team_name, goals, assists, appearances
        FROM computed_player_stats
        WHERE league_id = %s AND season = %s AND goals > 0
        ORDER BY goals DESC, assists DESC
    """, (league_id, season))

    scorers = c.fetchall()

    if not scorers:
        print(f"  No scorer data for league {league_id} - player stats might not be computed yet")
        c.close()
        conn.close()
        return 0

    # clear old data for this league/season
    c.execute("DELETE FROM computed_top_scorers WHERE league_id = %s AND season = %s",
              (league_id, season))

    rank = 0
    prev_goals = -1

    for row in scorers:
        player_id, name, team_id, team_name, goals, assists, appearances = row

        # ranking logic - same goals = same rank
        if goals != prev_goals:
            rank += 1
            prev_goals = goals

        # calculate minutes per goal
        # we don't track actual minutes so estimate: appearances * 90
        est_minutes = (appearances or 1) * 90
        mpg = round(est_minutes / goals, 1) if goals > 0 else None

        # penalty goals - we can get this from events
        # but its slow so skip for now
        # TODO: add penalty goal tracking
        penalty_goals = 0

        try:
            c.execute("""
                INSERT INTO computed_top_scorers
                    (player_id, player_name, team_id, team_name, league_id, season,
                     goals, assists, penalty_goals, matches_played, minutes_per_goal,
                     rank_in_league, last_computed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                player_id, name, team_id, team_name, league_id, season,
                goals, assists, penalty_goals, appearances, mpg, rank
            ))
        except Exception as e:
            print(f"    Error for {name}: {e}")
            conn.rollback()

    conn.commit()
    c.close()
    conn.close()

    return len(scorers)


### MAIN ###

if __name__ == "__main__":
    print(f"[{datetime.now()}] Top scorers computation")
    print("-" * 40)

    for cfg in LEAGUE_CONFIGS:
        try:
            n = compute_scorers(cfg["id"], cfg["season"])
            print(f"  {cfg['name']}: {n} scorers ranked")
        except Exception as err:
            print(f"  FAILED {cfg['name']}: {err}")

    print(f"[{datetime.now()}] Done")
