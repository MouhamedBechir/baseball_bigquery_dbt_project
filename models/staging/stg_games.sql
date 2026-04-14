{{
  config(
    materialized='view'
  )
}}

/*
  stg_games
  ─────────
  Cleaned and standardised game facts.
  - Renamed columns to snake_case
  - Cast data types
  - Derived fields: winning_team, win_type, total_runs, run_differential
*/

WITH source AS (
    SELECT * FROM {{ ref('raw_games_wide') }}
),

cleaned AS (
    SELECT
        -- Identifiers
        gameId                                              AS game_id,

        -- Dates & time
        DATE(startTime)                                     AS game_date,
        EXTRACT(YEAR FROM startTime)                        AS season_year,
        EXTRACT(MONTH FROM startTime)                       AS season_month,
        startTime                                           AS game_timestamp,

        -- Teams
        homeTeamName                                        AS home_team,
        awayTeamName                                        AS away_team,

        -- Scores
        CAST(homeCurrentTotalRuns AS INT64)                         AS home_runs,
        CAST(awayCurrentTotalRuns AS INT64)                         AS away_runs,

        -- Derived: winner
        CASE
            WHEN homeCurrentTotalRuns > awayCurrentTotalRuns THEN homeTeamName
            WHEN awayCurrentTotalRuns > homeCurrentTotalRuns THEN awayTeamName
            ELSE 'TIE'
        END                                                 AS winning_team,

        -- Derived: home/away advantage
        CASE
            WHEN homeCurrentTotalRuns > awayCurrentTotalRuns THEN 'HOME'
            WHEN awayCurrentTotalRuns > homeCurrentTotalRuns THEN 'AWAY'
            ELSE 'TIE'
        END                                                 AS win_type,

        -- Derived: run metrics
        (CAST(homeCurrentTotalRuns AS INT64) + CAST(awayCurrentTotalRuns AS INT64))     AS total_runs,
        ABS(CAST(homeCurrentTotalRuns AS INT64) - CAST(awayCurrentTotalRuns AS INT64))  AS run_differential,

        -- Venue
        venueId                                             AS venue_id,
        venueName                                           AS venue_name,

        -- Game metadata
        attendance,
        durationMinutes

    FROM source
    WHERE gameId IS NOT NULL
      AND startTime IS NOT NULL
)

SELECT * FROM cleaned
