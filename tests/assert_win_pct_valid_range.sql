-- Test: assert_win_pct_valid_range
-- Fails if any team has a win percentage outside [0, 1]

SELECT
    team,
    season_year,
    win_pct
FROM {{ ref('agg_team_season_stats') }}
WHERE win_pct < 0
   OR win_pct > 1
