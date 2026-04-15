{{
  config(
    materialized='view'
  )
}}

SELECT
    gameId          AS game_id,
    DATE(startTime) AS game_date,
    homeTeamName    AS home_team,
    awayTeamName    AS away_team,
    status

FROM {{ ref('raw_schedules') }}
WHERE gameId IS NOT NULL
