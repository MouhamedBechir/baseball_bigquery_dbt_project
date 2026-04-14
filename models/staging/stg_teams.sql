{{
  config(
    materialized='table',
    tags=['static']
  )
}}

/*
  stg_teams
  ─────────
  Derives a unique list of all teams from games data.
  Flags Red Sox for easy filtering downstream.
*/

WITH home_teams AS (
    SELECT DISTINCT
        homeTeamName    AS team_name,
        homeTeamId      AS team_id
    FROM {{ ref('raw_games_wide') }}
    WHERE homeTeamName IS NOT NULL
),

away_teams AS (
    SELECT DISTINCT
        awayTeamName    AS team_name,
        awayTeamId      AS team_id
    FROM {{ ref('raw_games_wide') }}
    WHERE awayTeamName IS NOT NULL
),

all_teams AS (
    SELECT * FROM home_teams
    UNION DISTINCT
    SELECT * FROM away_teams
)

SELECT
    team_id,
    team_name,
    CASE
        WHEN team_name IN ('Red Sox') THEN TRUE
        ELSE FALSE
    END AS is_redsox

FROM all_teams
