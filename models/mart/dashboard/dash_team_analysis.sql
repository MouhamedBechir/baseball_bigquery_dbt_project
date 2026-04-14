{{
  config(
    materialized='table',
    tags=['dashboard', 'redsox', 'performance', 'looker_studio']
  )
}}


WITH team_stats AS (
    SELECT 
        season_year,
        team,
        total_games,
        total_runs_scored,
        avg_runs_scored,
        total_wins,
        
        -- Calculate performance tier based on runs scored
        CASE 
            WHEN avg_runs_scored >= 5.5 THEN 'Excellent Offense'
            WHEN avg_runs_scored >= 4.5 THEN 'Good Offense'
            WHEN avg_runs_scored >= 4.0 THEN 'Average Offense'
            WHEN avg_runs_scored >= 3.5 THEN 'Below Average Offense'
            ELSE 'Poor Offense'
        END AS performance_tier,
        
        -- Games played classification
        CASE 
            WHEN total_games >= 162 THEN 'Full Season'
            WHEN total_games >= 100 THEN 'Most Season'
            WHEN total_games >= 50 THEN 'Half Season'
            ELSE 'Limited Games'
        END AS games_tier,
        
        -- Win rate classification
        CASE 
            WHEN (CAST(total_wins AS FLOAT64) / total_games) >= 0.600 THEN 'Elite Win Rate'
            WHEN (CAST(total_wins AS FLOAT64) / total_games) >= 0.500 THEN 'Excellent Win Rate'
            WHEN (CAST(total_wins AS FLOAT64) / total_games) >= 0.400 THEN 'Above Average Win Rate'
            ELSE 'Poor Win Rate'
        END AS win_rate_tier,
        
        -- Calculate win percentage
        CAST(total_wins AS FLOAT64) / total_games AS win_pct,
        
        -- Rename for consistency
        avg_runs_scored AS avg_runs_per_game
        
    FROM {{ ref('agg_team_stats') }}
),

league_averages AS (
    SELECT 
        season_year,
        AVG(avg_runs_per_game) AS league_avg_runs,
        AVG(win_pct) AS league_avg_win_pct,
        STDDEV(avg_runs_per_game) AS league_runs_std,
        COUNT(DISTINCT team) AS total_teams
    FROM team_stats
    WHERE total_games >= 50  -- Only teams with significant games
    GROUP BY season_year
),

redsox_stats AS (
    SELECT 
        season_year,
        COUNT(DISTINCT team) AS redsox_teams_count,
        AVG(avg_runs_per_game) AS redsox_avg_runs,
        AVG(win_pct) AS redsox_avg_win_pct,
        SUM(total_games) AS redsox_total_games
    FROM team_stats
    WHERE team = 'Boston Red Sox' AND total_games >= 30
    GROUP BY season_year
)

SELECT 
    ts.season_year,
    ts.team,
    ts.total_games,
    ts.total_wins,
    ts.total_runs_scored,
    ts.avg_runs_per_game,
    ts.win_pct,
    
    -- Performance classifications
    ts.performance_tier,
    ts.games_tier,
    ts.win_rate_tier,
    
    -- League comparisons
    la.league_avg_runs,
    la.league_avg_win_pct,
    ts.avg_runs_per_game - la.league_avg_runs AS runs_vs_league_avg,
    ts.win_pct - la.league_avg_win_pct AS win_pct_vs_league_avg,
    
    -- Performance relative to league (in standard deviations)
    (ts.avg_runs_per_game - la.league_avg_runs) / NULLIF(la.league_runs_std, 0) AS runs_std_devs_from_avg,
    
    -- Red Sox specific metrics
    CASE 
        WHEN ts.team = 'Red Sox' THEN 'Red Sox Team'
        ELSE 'Non-Red Sox'
    END AS team_affiliation,
    
    -- Red Sox team context
    rs.redsox_teams_count,
    rs.redsox_avg_runs,
    rs.redsox_avg_win_pct,
    rs.redsox_total_games,
    
    -- Individual vs Red Sox team average
    CASE 
        WHEN ts.team = 'Red Sox' THEN 
            ts.avg_runs_per_game - rs.redsox_avg_runs
        ELSE NULL
    END AS runs_vs_redsox_avg,
    
    CASE 
        WHEN ts.team = 'Red Sox' THEN 
            ts.win_pct - rs.redsox_avg_win_pct
        ELSE NULL
    END AS win_pct_vs_redsox_avg,
    
    -- Quality metrics
    CASE 
        WHEN ts.total_games >= 150 AND ts.avg_runs_per_game >= 5.0 THEN 'Elite Team'
        WHEN ts.total_games >= 140 AND ts.avg_runs_per_game >= 4.5 THEN 'Strong Team'
        WHEN ts.total_games >= 120 AND ts.avg_runs_per_game >= 4.0 THEN 'Good Team'
        WHEN ts.total_games >= 100 THEN 'Average Team'
        ELSE 'Below Average Team'
    END AS team_grade,
    
    -- Consistency indicator (high runs + good win rate)
    CASE 
        WHEN ts.avg_runs_per_game >= 5.0 AND ts.win_pct >= 0.600 THEN 'Elite Consistency'
        WHEN ts.avg_runs_per_game >= 4.5 AND ts.win_pct >= 0.500 THEN 'Good Consistency'
        WHEN ts.avg_runs_per_game >= 4.0 AND ts.win_pct >= 0.400 THEN 'Average Consistency'
        ELSE 'Inconsistent'
    END AS consistency_rating,
    
    CURRENT_TIMESTAMP() AS loaded_at

FROM team_stats ts
LEFT JOIN league_averages la ON ts.season_year = la.season_year
LEFT JOIN redsox_stats rs ON ts.season_year = rs.season_year
WHERE ts.total_games >= 30  -- Minimum threshold for inclusion
ORDER BY ts.season_year DESC, ts.avg_runs_per_game DESC
