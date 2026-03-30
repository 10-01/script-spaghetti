# Football Data Pipeline

Scripts for pulling football/soccer data from API-Football and computing stats.

## Setup

1. Copy `.env.example` to `.env` and add your API key
2. Run `docker-compose up -d` to start Postgres
3. Run `python3 db_setup.py` to create tables (or just use the SQL file)
4. Install deps: `pip install -r requirements.txt`

## Running

The crontab file has the schedule. Basically:
- Pull scripts run at 2am ET
- Compute scripts run at 3:30am ET

You can also run `./run_daily.sh` manually.

## Leagues

- Premier League
- La Liga
- Serie A
- MLS

## Notes

- Ask Dave if you need the API key, he has it somewhere
- The compute scripts need the pull scripts to finish first obviously
- If MLS data looks weird its because their season is different from european leagues
- Check #data-eng on Slack if something breaks
