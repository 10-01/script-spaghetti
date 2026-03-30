#!/usr/bin/env python3
"""
compute_league_trends.py

Calculates:
- Last 5 match form for each team
- Current streak (win/draw/loss)
- Points and goals in last 5 matches
- Position change vs previous run

This is the most complex compute script and honestly could use a rewrite.
The position_change calculation doesn't really work right because we
truncate and re-insert every time. TODO fix this someday.
"""
import psycopg2
from datetime import datetime
import json

# Mix of imports to demonstrate inconsistency
from helpers import getDbConnection, runQuery, safe_int
from config import LEAGUES, CURRENT_SEASON, MLS_SEASON, get_season_for_league

print(f"--- League Trends Computation ---")
print(f"--- Started: {datetime.now()} ---")


def get_recent_fixtures(conn, team_id, league_id, season, limit=5):
    """Get the most recent N fixtures for a team, ordered by date desc."""
    cur = conn.cursor()
    cur.execute("""
        SELECT fixture_id, date, home_team_id, away_team_id, home_goals, away_goals
        FROM raw_fixtures
        WHERE (home_team_id = %s OR away_team_id = %s)
        AND league_id = %s AND season = %s
        AND status_short = 'FT'
        ORDER BY date DESC
        LIMIT %s
    """, (team_id, team_id, league_id, season, limit))
    rows = cur.fetchall()
    cur.close()
    return rows


def calc_form(team_id, fixtures):
    """Calculate W/D/L form string from recent fixtures."""
    form = ""
    for fix in fixtures:
        fid, date, home_id, away_id, hg, ag = fix
        hg = hg or 0
        ag = ag or 0

        if team_id == home_id:
            # home team
            if hg > ag:
                form += "W"
            elif hg < ag:
                form += "L"
            else:
                form += "D"
        else:
            # away team
            if ag > hg:
                form += "W"
            elif ag < hg:
                form += "L"
            else:
                form += "D"

    return form


def calc_streak(form):
    """Get current streak from form string. Returns (type, count)."""
    if not form:
        return ("", 0)

    streak_type = form[0]
    count = 0
    for ch in form:
        if ch == streak_type:
            count += 1
        else:
            break

    return (streak_type, count)


def calc_points_and_goals(team_id, fixtures):
    """Calculate points and goals from fixtures."""
    points = 0
    goals_for = 0
    goals_against = 0

    for fix in fixtures:
        fid, date, home_id, away_id, hg, ag = fix
        hg = hg or 0
        ag = ag or 0

        if team_id == home_id:
            goals_for += hg
            goals_against += ag
            if hg > ag: points += 3
            elif hg == ag: points += 1
        else:
            goals_for += ag
            goals_against += hg
            if ag > hg: points += 3
            elif ag == hg: points += 1

    return points, goals_for, goals_against


def compute_trends_for_league(league_id, season):
    conn = getDbConnection()
    cur = conn.cursor()

    # get all teams in this league from standings
    cur.execute("""
        SELECT DISTINCT team_id, team_name FROM raw_standings
        WHERE league_id = %s AND season = %s
    """, (league_id, season))

    teams = cur.fetchall()
    print(f"  League {league_id}: processing {len(teams)} teams")

    results = []

    for team_id, team_name in teams:
        fixtures = get_recent_fixtures(conn, team_id, league_id, season, limit=5)

        if not fixtures:
            continue

        form = calc_form(team_id, fixtures)
        streak_type, streak_count = calc_streak(form)
        points, goals, conceded = calc_points_and_goals(team_id, fixtures)

        results.append({
            'team_id': team_id,
            'team_name': team_name,
            'form': form[:5],  # cap at 5
            'streak_type': streak_type,
            'streak_count': streak_count,
            'points': points,
            'goals': goals,
            'conceded': conceded,
        })

    # truncate and reinsert (yeah yeah, I know)
    # the upsert was getting weird with the position_change calc so we just redo it
    cur.execute("DELETE FROM computed_league_trends WHERE league_id = %s AND season = %s",
                (league_id, season))
    conn.commit()

    for r in results:
        try:
            cur.execute("""
                INSERT INTO computed_league_trends
                    (team_id, team_name, league_id, season, last_5_form,
                     current_streak_type, current_streak_count,
                     points_last_5, goals_last_5, conceded_last_5,
                     position_change, last_computed)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
            """, (
                r['team_id'], r['team_name'], league_id, season,
                r['form'], r['streak_type'], r['streak_count'],
                r['points'], r['goals'], r['conceded'],
                0,  # position_change is always 0 because we truncate lol
            ))
        except Exception as e:
            print(f"    Error for {r['team_name']}: {e}")
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()

    return len(results)


# ---- main ----

if __name__ == "__main__":
    for leagueName, leagueId in LEAGUES.items():
        szn = get_season_for_league(leagueId)
        try:
            count = compute_trends_for_league(leagueId, szn)
            print(f"  {leagueName}: {count} teams computed")
        except Exception as e:
            print(f"  ERROR in {leagueName}: {e}")
            # keep going

    print(f"--- Finished: {datetime.now()} ---")
