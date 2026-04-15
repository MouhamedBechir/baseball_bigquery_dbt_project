{{
  config(
    materialized='incremental',
    unique_key='game_id',
    partition_by={
      "field": "game_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["season_year", "home_team", "away_team"],
    tags=['dashboard', 'postseason', 'playoffs', 'redsox', 'looker_studio']
  )
}}


SELECT
    -- Game identifiers
    g.game_id,
    g.game_date,
    g.season_year,
    
    -- Date dimensions
    d.year,
    d.month_name,
    d.quarter,
    d.quarter_label,
    d.day_name,
    d.is_weekend,
    
    -- Teams and context
    g.home_team,
    g.away_team,
    g.home_is_redsox,
    g.away_is_redsox,
    g.home_team_division,
    g.away_team_division,
    
    -- Red Sox specific flags
    CASE 
        WHEN g.home_is_redsox = TRUE THEN 'Home'
        WHEN g.away_is_redsox = TRUE THEN 'Away'
        ELSE 'Not Red Sox Game'
    END AS redsox_location,
    
    CASE 
        WHEN g.home_is_redsox = TRUE THEN g.away_team
        WHEN g.away_is_redsox = TRUE THEN g.home_team
        ELSE NULL
    END AS opponent,
    
    -- Game results
    g.home_runs,
    g.away_runs,
    g.winning_team,
    g.game_type,
    
    -- Red Sox specific results
    CASE 
        WHEN g.home_is_redsox = TRUE AND g.winning_team = g.home_team THEN 'Win'
        WHEN g.away_is_redsox = TRUE AND g.winning_team = g.away_team THEN 'Win'
        WHEN g.home_is_redsox = TRUE OR g.away_is_redsox = TRUE THEN 'Loss'
        ELSE 'Not Red Sox Game'
    END AS redsox_result,
    
    -- Score when Red Sox playing
    CASE 
        WHEN g.home_is_redsox = TRUE THEN g.home_runs
        WHEN g.away_is_redsox = TRUE THEN g.away_runs
        ELSE NULL
    END AS redsox_runs,
    
    CASE 
        WHEN g.home_is_redsox = TRUE THEN g.away_runs
        WHEN g.away_is_redsox = TRUE THEN g.home_runs
        ELSE NULL
    END AS opponent_runs,
    
    -- Run differential
    CASE 
        WHEN g.home_is_redsox = TRUE THEN g.home_runs - g.away_runs
        WHEN g.away_is_redsox = TRUE THEN g.away_runs - g.home_runs
        ELSE NULL
    END AS redsox_run_differential,
    
    -- Game categorization
    CASE 
        WHEN g.game_type IN ('WC', 'WC-G') THEN 'Wild Card'
        WHEN g.game_type IN ('DS', 'DS-G') THEN 'Division Series'
        WHEN g.game_type IN ('CS', 'CS-G') THEN 'Championship Series'
        WHEN g.game_type IN ('WS', 'WS-G') THEN 'World Series'
        ELSE 'Other'
    END AS playoff_round,
    
    -- Competitiveness
    CASE 
        WHEN ABS(g.home_runs - g.away_runs) >= 10 THEN 'Blowout'
        WHEN ABS(g.home_runs - g.away_runs) >= 5 THEN 'Comfortable Win'
        WHEN ABS(g.home_runs - g.away_runs) >= 2 THEN 'Close Game'
        ELSE 'Very Close'
    END AS competitive_category,
    
    -- Scoring category
    CASE 
        WHEN g.home_runs + g.away_runs <= 5 THEN 'Low Scoring'
        WHEN g.home_runs + g.away_runs <= 10 THEN 'Normal Scoring'
        WHEN g.home_runs + g.away_runs <= 15 THEN 'High Scoring'
        ELSE 'Very High Scoring'
    END AS scoring_category,
    
    -- Venue information
    g.venue_id,
    g.venue_name,
    
    -- Season success indicator
    CASE 
        WHEN g.winning_team = 'Red Sox' AND g.game_type = 'WS' THEN 'Won World Series'
        WHEN g.winning_team = 'Red Sox' AND g.game_type IN ('CS', 'CS-G') THEN 'Won League Championship'
        WHEN g.winning_team = 'Red Sox' AND g.game_type IN ('DS', 'DS-G') THEN 'Won Division Series'
        WHEN g.winning_team = 'Red Sox' AND g.game_type IN ('WC', 'WC-G') THEN 'Won Wild Card'
        WHEN g.home_is_redsox = TRUE OR g.away_is_redsox = TRUE THEN 'Lost Playoff Game'
        ELSE 'Non-Red Sox Playoff Game'
    END AS game_significance,
    
    CURRENT_TIMESTAMP() AS loaded_at

FROM {{ ref('fct_postseason_games') }} g
LEFT JOIN {{ ref('dim_dates') }} d ON g.game_date = d.date
WHERE g.home_is_redsox = TRUE OR g.away_is_redsox = TRUE

{% if is_incremental() %}
    AND g.game_date > (
        SELECT MAX(game_date) FROM {{ this }}
    )
{% endif %}
