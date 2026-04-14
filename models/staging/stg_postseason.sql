{{
  config(
    materialized='view'
  )
}}

/*
  stg_postseason
  ──────────────
  Cleaned postseason game data, aligned to same structure as stg_games.
*/

SELECT
    gameId                                          AS game_id,
    DATE(startTime)                                 AS game_date,
    EXTRACT(YEAR FROM startTime)                    AS season_year,
    homeTeamName                                    AS home_team,
    awayTeamName                                    AS away_team,
    CAST(homeCurrentTotalRuns AS INT64)                     AS home_runs,
    CAST(awayCurrentTotalRuns AS INT64)                     AS away_runs,

    CASE
        WHEN homeCurrentTotalRuns > awayCurrentTotalRuns THEN homeTeamName
        WHEN awayCurrentTotalRuns > homeCurrentTotalRuns THEN awayTeamName
        ELSE 'TIE'
    END                                             AS winning_team,

    'POSTSEASON'                                    AS game_type

FROM {{ ref('raw_games_post_wide') }}
WHERE gameId IS NOT NULL
  AND startTime IS NOT NULL
