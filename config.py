import os
from dotenv import load_dotenv

load_dotenv()

# API Football config
API_KEY = os.getenv("API_FOOTBALL_KEY", "YOUR_API_KEY_HERE")
API_BASE_URL = "https://v3.football.api-sports.io"

# Database config
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "football_data")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASS = os.getenv("DB_PASSWORD", "postgres")

# Leagues we track
# NOTE: MLS season is calendar year, european leagues are split across years
LEAGUES = {
    "premier_league": 39,
    "la_liga": 140,
    "serie_a": 135,
    "mls": 253,
}

# current season - update this every year
# TODO: make this dynamic somehow
CURRENT_SEASON = 2025
MLS_SEASON = 2026  # MLS uses calendar year

def get_season_for_league(league_id):
    if league_id == 253:
        return MLS_SEASON
    return CURRENT_SEASON

def get_db_connection_string():
    return f"host={DB_HOST} port={DB_PORT} dbname={DB_NAME} user={DB_USER} password={DB_PASS}"

# headers for API calls
def get_headers():
    return {
        "x-apisports-key": API_KEY,
        "x-apisports-host": "v3.football.api-sports.io"
    }
