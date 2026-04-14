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
    tags=['dashboard', 'redsox', 'games', 'analysis', 'looker_studio']
  )
}}

/*
  dash_game_analysis
  ==================
  Detailed Game Analysis for Looker Studio
  All games with Red Sox involvement - dashboard page 2
*/

SELECT
    -- Game identifiers
    g.game_id,
    g.game_date,
    g.season_year,
    g.season_month,
    
    -- Date dimensions (for filtering)
    d.year,
    d.month_name,
    d.month_short,
    d.quarter,
    d.quarter_label,
    d.day_name,
    d.day_short,
    d.is_weekend,
    d.is_baseball_season,
    
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
    
    -- Opponent (when Red Sox playing)
    CASE 
        WHEN g.home_is_redsox = TRUE THEN g.away_team
        WHEN g.away_is_redsox = TRUE THEN g.home_team
        ELSE NULL
    END AS opponent,
    
    -- Game results
    g.home_runs,
    g.away_runs,
    g.total_runs,
    g.run_differential,
    g.winning_team,
    g.win_type,
    
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
    
    -- Venue information
    g.venue_id,
    g.venue_name,
    g.attendance,
    g.durationMinutes,
    
    -- Game metadata
    g.game_type,
    
    -- Performance categorization
    CASE 
        WHEN g.total_runs <= 5 THEN 'Low Scoring'
        WHEN g.total_runs <= 10 THEN 'Normal Scoring'
        WHEN g.total_runs <= 15 THEN 'High Scoring'
        ELSE 'Very High Scoring'
    END AS scoring_category,
    
    CASE 
        WHEN ABS(g.run_differential) >= 10 THEN 'Blowout'
        WHEN ABS(g.run_differential) >= 5 THEN 'Comfortable Win'
        WHEN ABS(g.run_differential) >= 2 THEN 'Close Game'
        ELSE 'Very Close'
    END AS competitive_category,
    
    CURRENT_TIMESTAMP() AS loaded_at

FROM {{ ref('fct_games') }} g
LEFT JOIN {{ ref('dim_dates') }} d ON g.game_date = d.date
WHERE g.home_is_redsox = TRUE OR g.away_is_redsox = TRUE

{% if is_incremental() %}
    AND g.game_date > (
        SELECT MAX(game_date) FROM {{ this }}
    )
{% endif %}
