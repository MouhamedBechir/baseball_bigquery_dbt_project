-- Test: assert_no_negative_runs
-- Fails if any game has negative run values
-- This would indicate a data quality issue in the source

SELECT
    game_id,
    home_runs,
    away_runs
FROM {{ ref('fct_games') }}
WHERE home_runs < 0
   OR away_runs < 0
