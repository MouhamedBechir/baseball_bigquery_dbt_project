{{
  config(
    materialized='table',
    tags=['dashboard', 'league', 'comparison', 'rankings', 'looker_studio']
  )
}}


WITH team_stats AS (
    SELECT 
        season_year,
        team,
        total_games,
        total_wins,
        total_losses,
        win_pct,
        total_runs_scored,
        total_runs_conceded,
        run_differential,
        home_games,
        away_games,
        season_rank,
        
        -- Calculate percentile rankings
        PERCENT_RANK() OVER (PARTITION BY season_year ORDER BY win_pct DESC) * 100 AS win_pct_percentile,
        PERCENT_RANK() OVER (PARTITION BY season_year ORDER BY total_runs_scored DESC) * 100 AS runs_scored_percentile,
        PERCENT_RANK() OVER (PARTITION BY season_year ORDER BY run_differential DESC) * 100 AS run_diff_percentile,
        
        -- League averages for comparison
        AVG(win_pct) OVER (PARTITION BY season_year) AS league_avg_win_pct,
        AVG(total_runs_scored) OVER (PARTITION BY season_year) AS league_avg_runs_scored,
        STDDEV(win_pct) OVER (PARTITION BY season_year) AS league_win_pct_std
        
    FROM {{ ref('agg_team_season_stats') }}
),

redsox_benchmark AS (
    SELECT 
        season_year,
        win_pct AS redsox_win_pct,
        total_runs_scored AS redsox_runs,
        run_differential AS redsox_run_diff,
        season_rank AS redsox_rank
    FROM team_stats
    WHERE team = 'Red Sox'
)

SELECT 
    ts.season_year,
    ts.team,
    ts.total_games,
    ts.total_wins,
    ts.total_losses,
    ts.win_pct,
    ts.total_runs_scored,
    ts.total_runs_conceded,
    ts.run_differential,
    ts.season_rank,
    
    -- Performance vs league average
    ts.win_pct - ts.league_avg_win_pct AS win_pct_vs_avg,
    ts.total_runs_scored - ts.league_avg_runs_scored AS runs_vs_avg,
    
    -- Standard deviations from mean
    (ts.win_pct - ts.league_avg_win_pct) / NULLIF(ts.league_win_pct_std, 0) AS win_pct_std_devs,
    
    -- Percentile rankings
    ts.win_pct_percentile,
    ts.runs_scored_percentile,
    ts.run_diff_percentile,
    
    -- Performance tiers
    CASE 
        WHEN ts.win_pct_percentile >= 90 THEN 'Elite'
        WHEN ts.win_pct_percentile >= 75 THEN 'Excellent'
        WHEN ts.win_pct_percentile >= 60 THEN 'Good'
        WHEN ts.win_pct_percentile >= 40 THEN 'Average'
        WHEN ts.win_pct_percentile >= 25 THEN 'Below Average'
        ELSE 'Poor'
    END AS performance_tier,
    
    -- Division and league context
    CASE 
        WHEN ts.season_rank <= 6 THEN 'Playoff Team'
        ELSE 'Non-Playoff'
    END AS playoff_status,
    
    -- Red Sox comparison
    rb.redsox_win_pct,
    rb.redsox_runs,
    rb.redsox_run_diff,
    rb.redsox_rank,
    
    -- Head-to-head comparison
    CASE 
        WHEN ts.team = 'Red Sox' THEN 'Red Sox'
        WHEN ts.win_pct > rb.redsox_win_pct THEN 'Better than Red Sox'
        WHEN ts.win_pct = rb.redsox_win_pct THEN 'Equal to Red Sox'
        ELSE 'Worse than Red Sox'
    END AS vs_redsox_comparison,
    
    -- Division flags (for filtering)
    CASE 
        WHEN ts.team IN ('Red Sox', 'Yankees', 'Blue Jays', 'Rays', 'Orioles') THEN 'AL East'
        WHEN ts.team IN ('White Sox', 'Indians', 'Tigers', 'Royals', 'Twins') THEN 'AL Central'
        WHEN ts.team IN ('Houston Astros', 'Los Angeles Angels', 'Oakland Athletics', 
                        'Seattle Mariners', 'Texas Rangers') THEN 'AL West'
        WHEN ts.team IN ('Atlanta Braves', 'Miami Marlins', 'New York Mets', 
                        'Philadelphia Phillies', 'Washington Nationals') THEN 'NL East'
        WHEN ts.team IN ('Chicago Cubs', 'Cincinnati Reds', 'Milwaukee Brewers', 
                        'Pittsburgh Pirates', 'St. Louis Cardinals') THEN 'NL Central'
        WHEN ts.team IN ('Arizona Diamondbacks', 'Colorado Rockies', 'Los Angeles Dodgers', 
                        'San Diego Padres', 'San Francisco Giants') THEN 'NL West'
        ELSE 'Unknown'
    END AS division,
    
    CASE 
        WHEN ts.team IN ('Boston Red Sox', 'New York Yankees', 'Toronto Blue Jays', 
                        'Tampa Bay Rays', 'Baltimore Orioles', 'Chicago White Sox', 
                        'Cleveland Guardians', 'Detroit Tigers', 'Kansas City Royals', 
                        'Minnesota Twins', 'Houston Astros', 'Los Angeles Angels', 
                        'Oakland Athletics', 'Seattle Mariners', 'Texas Rangers') THEN 'American League'
        ELSE 'National League'
    END AS league,
    
    CURRENT_TIMESTAMP() AS loaded_at

FROM team_stats ts
LEFT JOIN redsox_benchmark rb ON ts.season_year = rb.season_year
ORDER BY ts.season_year DESC, ts.season_rank ASC
