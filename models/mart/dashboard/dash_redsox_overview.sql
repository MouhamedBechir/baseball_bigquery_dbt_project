{{
  config(
    materialized='table',
    tags=['dashboard', 'redsox', 'overview', 'kpi', 'looker_studio']
  )
}}



SELECT
    season_year,
    total_games,
    total_wins,
    total_losses,
    win_pct,
    total_runs_scored,
    total_runs_conceded,
    run_differential,
    home_games,
    away_games,
    avg_home_attendance,
    total_home_attendance,
    season_rank AS league_rank,
    
    -- Trend calculations
    win_pct - LAG(win_pct) OVER (ORDER BY season_year) AS win_pct_change,
    win_pct - LAG(win_pct, 2) OVER (ORDER BY season_year) AS win_pct_2yr_change,
    
    -- Rolling averages
    AVG(win_pct) OVER (
        ORDER BY season_year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS win_pct_3yr_avg,
    
    AVG(total_runs_scored) OVER (
        ORDER BY season_year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS runs_scored_3yr_avg,
    
    -- Performance classification
    CASE 
        WHEN win_pct >= 0.600 THEN 'Excellent'
        WHEN win_pct >= 0.550 THEN 'Good'
        WHEN win_pct >= 0.500 THEN 'Average'
        WHEN win_pct >= 0.450 THEN 'Below Average'
        ELSE 'Poor'
    END AS performance_tier,
    
    -- Playoff qualification (simplified)
    CASE 
        WHEN season_rank <= 6 THEN 'Made Playoffs'
        ELSE 'Missed Playoffs'
    END AS playoff_status,
    
    CURRENT_TIMESTAMP() AS loaded_at

FROM {{ ref('agg_team_season_stats') }}
WHERE team = 'Red Sox'
ORDER BY season_year DESC
