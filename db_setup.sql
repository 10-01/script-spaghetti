-- Football Data Pipeline - Database Setup
-- Run this before starting the pipeline
-- Last updated: 2024-08-15 by dave

CREATE TABLE IF NOT EXISTS raw_fixtures (
    id SERIAL PRIMARY KEY,
    fixture_id INTEGER UNIQUE NOT NULL,
    league_id INTEGER NOT NULL,
    league_name VARCHAR(100),
    season INTEGER,
    round VARCHAR(50),
    date TIMESTAMP,
    venue_name VARCHAR(200),
    venue_city VARCHAR(100),
    home_team_id INTEGER,
    home_team_name VARCHAR(100),
    away_team_id INTEGER,
    away_team_name VARCHAR(100),
    home_goals INTEGER,
    away_goals INTEGER,
    home_goals_ht INTEGER,
    away_goals_ht INTEGER,
    status_short VARCHAR(10),
    status_long VARCHAR(50),
    referee VARCHAR(100),
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_standings (
    id SERIAL PRIMARY KEY,
    league_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    team_name VARCHAR(100),
    rank INTEGER,
    points INTEGER,
    games_played INTEGER,
    wins INTEGER,
    draws INTEGER,
    losses INTEGER,
    goals_for INTEGER,
    goals_against INTEGER,
    goal_diff INTEGER,
    form VARCHAR(20),
    description VARCHAR(200),
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    updated_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(league_id, season, team_id)
);

CREATE TABLE IF NOT EXISTS raw_teams (
    id SERIAL PRIMARY KEY,
    team_id INTEGER UNIQUE NOT NULL,
    name VARCHAR(100),
    code VARCHAR(10),
    country VARCHAR(50),
    founded INTEGER,
    logo_url VARCHAR(500),
    venue_name VARCHAR(200),
    venue_capacity INTEGER,
    venue_city VARCHAR(100),
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_players (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    team_id INTEGER NOT NULL,
    name VARCHAR(100),
    age INTEGER,
    nationality VARCHAR(50),
    position VARCHAR(30),
    photo_url VARCHAR(500),
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW(),
    UNIQUE(player_id, team_id)
);

CREATE TABLE IF NOT EXISTS raw_events (
    id SERIAL PRIMARY KEY,
    fixture_id INTEGER NOT NULL,
    time_elapsed INTEGER,
    time_extra INTEGER,
    team_id INTEGER,
    team_name VARCHAR(100),
    player_id INTEGER,
    player_name VARCHAR(100),
    assist_id INTEGER,
    assist_name VARCHAR(100),
    event_type VARCHAR(50),
    detail VARCHAR(100),
    comments VARCHAR(500),
    raw_json JSONB,
    created_at TIMESTAMP DEFAULT NOW()
);

-- computed tables

CREATE TABLE IF NOT EXISTS computed_player_stats (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(100),
    team_id INTEGER,
    team_name VARCHAR(100),
    league_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    yellow_cards INTEGER DEFAULT 0,
    red_cards INTEGER DEFAULT 0,
    appearances INTEGER DEFAULT 0,
    goals_per_game FLOAT DEFAULT 0,
    minutes_played INTEGER DEFAULT 0,
    last_computed TIMESTAMP DEFAULT NOW(),
    UNIQUE(player_id, league_id, season)
);

CREATE TABLE IF NOT EXISTS computed_team_stats (
    id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL,
    team_name VARCHAR(100),
    league_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    total_matches INTEGER DEFAULT 0,
    home_wins INTEGER DEFAULT 0,
    home_draws INTEGER DEFAULT 0,
    home_losses INTEGER DEFAULT 0,
    away_wins INTEGER DEFAULT 0,
    away_draws INTEGER DEFAULT 0,
    away_losses INTEGER DEFAULT 0,
    goals_scored INTEGER DEFAULT 0,
    goals_conceded INTEGER DEFAULT 0,
    clean_sheets INTEGER DEFAULT 0,
    avg_goals_scored FLOAT DEFAULT 0,
    avg_goals_conceded FLOAT DEFAULT 0,
    home_goals_scored INTEGER DEFAULT 0,
    away_goals_scored INTEGER DEFAULT 0,
    last_computed TIMESTAMP DEFAULT NOW(),
    UNIQUE(team_id, league_id, season)
);

CREATE TABLE IF NOT EXISTS computed_league_trends (
    id SERIAL PRIMARY KEY,
    team_id INTEGER NOT NULL,
    team_name VARCHAR(100),
    league_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    last_5_form VARCHAR(10),
    current_streak_type VARCHAR(5),  -- W, D, L
    current_streak_count INTEGER DEFAULT 0,
    points_last_5 INTEGER DEFAULT 0,
    goals_last_5 INTEGER DEFAULT 0,
    conceded_last_5 INTEGER DEFAULT 0,
    position_change INTEGER DEFAULT 0,  -- vs last week
    last_computed TIMESTAMP DEFAULT NOW(),
    UNIQUE(team_id, league_id, season)
);

CREATE TABLE IF NOT EXISTS computed_top_scorers (
    id SERIAL PRIMARY KEY,
    player_id INTEGER NOT NULL,
    player_name VARCHAR(100),
    team_id INTEGER,
    team_name VARCHAR(100),
    league_id INTEGER NOT NULL,
    season INTEGER NOT NULL,
    goals INTEGER DEFAULT 0,
    assists INTEGER DEFAULT 0,
    penalty_goals INTEGER DEFAULT 0,
    matches_played INTEGER DEFAULT 0,
    minutes_per_goal FLOAT,
    rank_in_league INTEGER,
    last_computed TIMESTAMP DEFAULT NOW(),
    UNIQUE(player_id, league_id, season)
);

-- indexes (added later by someone, not sure who)
CREATE INDEX IF NOT EXISTS idx_fixtures_league ON raw_fixtures(league_id);
CREATE INDEX IF NOT EXISTS idx_fixtures_date ON raw_fixtures(date);
CREATE INDEX IF NOT EXISTS idx_events_fixture ON raw_events(fixture_id);
CREATE INDEX IF NOT EXISTS idx_standings_league_season ON raw_standings(league_id, season);
-- CREATE INDEX idx_players_team ON raw_players(team_id);  -- removed, was causing issues with bulk inserts
