#!/usr/bin/env python3
"""
Compute aggregated player statistics from raw event data.

Calculates: goals, assists, cards, appearances per player per league.
"""
import psycopg2
from datetime import datetime
from helpers import getDbConnection, truncate_table, get_all_leagues, safe_int, safe_float

print(f"[{datetime.now()}] Computing player stats...")


def compute_for_league(conn, league_id, season):
    """Compute player stats for a single league."""
    cur = conn.cursor()

    # Get all events grouped by player for this league's fixtures
    cur.execute("""
        SELECT
            e.player_id,
            e.player_name,
            e.team_id,
            e.team_name,
            e.event_type,
            e.detail,
            COUNT(*) as cnt
        FROM raw_events e
        JOIN raw_fixtures f ON e.fixture_id = f.fixture_id
        WHERE f.league_id = %s AND f.season = %s
        AND e.player_id IS NOT NULL
        GROUP BY e.player_id, e.player_name, e.team_id, e.team_name, e.event_type, e.detail
    """, (league_id, season))

    events = cur.fetchall()

    # aggregate by player
    players = {}
    for row in events:
        pid = row[0]
        if pid not in players:
            players[pid] = {
                'player_id': pid,
                'player_name': row[1],
                'team_id': row[2],
                'team_name': row[3],
                'goals': 0,
                'assists': 0,
                'yellow_cards': 0,
                'red_cards': 0,
            }

        event_type = row[4]
        detail = row[5]
        count = row[6]

        if event_type == 'Goal':
            if detail != 'Missed Penalty':
                players[pid]['goals'] += count
        elif event_type == 'Card':
            if detail == 'Yellow Card':
                players[pid]['yellow_cards'] += count
            elif detail == 'Red Card':
                players[pid]['red_cards'] += count

    # now get appearances (distinct fixtures per player)
    cur.execute("""
        SELECT e.player_id, COUNT(DISTINCT e.fixture_id) as appearances
        FROM raw_events e
        JOIN raw_fixtures f ON e.fixture_id = f.fixture_id
        WHERE f.league_id = %s AND f.season = %s
        AND e.player_id IS NOT NULL
        GROUP BY e.player_id
    """, (league_id, season))

    for row in cur.fetchall():
        pid = row[0]
        if pid in players:
            players[pid]['appearances'] = row[1]

    # get assists from assist column
    cur.execute("""
        SELECT e.assist_id, COUNT(*) as assist_count
        FROM raw_events e
        JOIN raw_fixtures f ON e.fixture_id = f.fixture_id
        WHERE f.league_id = %s AND f.season = %s
        AND e.assist_id IS NOT NULL
        AND e.event_type = 'Goal'
        GROUP BY e.assist_id
    """, (league_id, season))

    for row in cur.fetchall():
        aid = row[0]
        if aid in players:
            players[aid]['assists'] += row[1]
        # NOTE: if the assist player doesn't have their own events, they won't
        # be in our dict. This means some assists get lost. Known issue, not critical.

    # insert computed stats
    for pid, stats in players.items():
        appearances = stats.get('appearances', 0)
        gpg = safe_float(stats['goals'] / appearances) if appearances > 0 else 0

        try:
            cur.execute("""
                INSERT INTO computed_player_stats
                    (player_id, player_name, team_id, team_name, league_id, season,
                     goals, assists, yellow_cards, red_cards, appearances, goals_per_game, last_computed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
                ON CONFLICT (player_id, league_id, season)
                DO UPDATE SET
                    goals = EXCLUDED.goals,
                    assists = EXCLUDED.assists,
                    yellow_cards = EXCLUDED.yellow_cards,
                    red_cards = EXCLUDED.red_cards,
                    appearances = EXCLUDED.appearances,
                    goals_per_game = EXCLUDED.goals_per_game,
                    last_computed = NOW()
            """, (
                pid, stats['player_name'], stats['team_id'], stats['team_name'],
                league_id, season,
                stats['goals'], stats['assists'], stats['yellow_cards'], stats['red_cards'],
                appearances, gpg
            ))
        except Exception as e:
            print(f"  Error inserting player stats for {stats['player_name']}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    return len(players)


if __name__ == "__main__":
    conn = getDbConnection()

    # get seasons we have data for
    # hardcoded for now
    seasons_by_league = {
        39: 2025,
        140: 2025,
        135: 2025,
        253: 2026,
    }

    for league_id in get_all_leagues():
        season = seasons_by_league.get(league_id, 2025)
        count = compute_for_league(conn, league_id, season)
        print(f"  League {league_id}: {count} players computed")

    conn.close()
    print(f"[{datetime.now()}] Player stats computation complete.")
