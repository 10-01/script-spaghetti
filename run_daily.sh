#!/bin/bash
#
# run_daily.sh - runs the full pipeline manually
# Use this instead of waiting for cron if you need fresh data now
#
# Usage: ./run_daily.sh
#
# NOTE: this takes a while because of API rate limits in pull_players.py
#

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

echo "========================================"
echo "Football Data Pipeline - Manual Run"
echo "Started: $(date)"
echo "========================================"

echo ""
echo ">>> Step 1: Pull fixtures"
python3 pull_fixtures.py

echo ""
echo ">>> Step 2: Pull standings"
python3 pull_standings.py

echo ""
echo ">>> Step 3: Pull teams"
python3 pull_teams.py

echo ""
echo ">>> Step 4: Pull players (this takes a while)"
python3 pull_players.py

echo ""
echo ">>> Step 5: Pull events"
python3 pull_events.py

echo ""
echo ">>> Step 6: Compute player stats"
python3 compute_player_stats.py

echo ""
echo ">>> Step 7: Compute team stats"
python3 compute_team_stats.py

echo ""
echo ">>> Step 8: Compute league trends"
python3 compute_league_trends.py

echo ""
echo ">>> Step 9: Compute top scorers"
python3 compute_top_scorers.py

echo ""
echo "========================================"
echo "Pipeline complete: $(date)"
echo "========================================"
