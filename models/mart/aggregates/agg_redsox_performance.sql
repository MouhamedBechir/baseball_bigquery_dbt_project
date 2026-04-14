{{
  config(
    materialized='table'
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
    season_rank                                         AS league_rank,

    -- Season-over-season change in win %
    win_pct - LAG(win_pct) OVER (ORDER BY season_year) AS win_pct_change,

    -- Rolling 3-year average win %
    AVG(win_pct) OVER (
        ORDER BY season_year
        ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )                                                   AS win_pct_3yr_avg

FROM {{ ref('agg_team_season_stats') }}
WHERE team = 'Red Sox'
ORDER BY season_year
