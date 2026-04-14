{{
  config(
    materialized='table'
  )
}}


WITH team_games AS (
    SELECT
        EXTRACT(YEAR FROM startTime)        AS season_year,
        homeTeamName                        AS team,
        'Home'                              AS location,
        COUNT(*)                            AS games_played,
        SUM(CAST(homeFinalRuns AS INT64))    AS runs_scored,
        AVG(CAST(homeFinalRuns AS INT64))    AS avg_runs_scored,
        COUNTIF(homeFinalRuns > awayFinalRuns) AS wins
    FROM {{ ref('raw_games_wide') }}
    WHERE homeTeamName IS NOT NULL
    GROUP BY 1, 2, 3

    UNION ALL

    SELECT
        EXTRACT(YEAR FROM startTime)        AS season_year,
        awayTeamName                        AS team,
        'Away'                              AS location,
        COUNT(*)                            AS games_played,
        SUM(CAST(awayFinalRuns AS INT64))    AS runs_scored,
        AVG(CAST(awayFinalRuns AS INT64))    AS avg_runs_scored,
        COUNTIF(awayFinalRuns > homeFinalRuns) AS wins
    FROM {{ ref('raw_games_wide') }}
    WHERE awayTeamName IS NOT NULL
    GROUP BY 1, 2, 3
)

SELECT
    season_year,
    team,
    SUM(games_played)                     AS total_games,
    SUM(runs_scored)                       AS total_runs_scored,
    AVG(avg_runs_scored)                 AS avg_runs_scored,
    SUM(wins)                              AS total_wins
FROM team_games
GROUP BY 1, 2
ORDER BY season_year DESC, total_wins DESC
