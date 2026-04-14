{{
  config(
    materialized='table'
  )
}}



WITH home_stats AS (
    SELECT
        season_year,
        home_team                                                           AS team,
        COUNT(*)                                                            AS home_games,
        SUM(CASE WHEN winning_team = home_team THEN 1 ELSE 0 END)          AS home_wins,
        SUM(home_runs)                                                      AS home_runs_scored,
        SUM(away_runs)                                                      AS home_runs_conceded,
        AVG(attendance)                                                     AS avg_home_attendance,
        SUM(attendance)                                                     AS total_home_attendance
    FROM {{ ref('fct_games') }}
    GROUP BY 1, 2
),

away_stats AS (
    SELECT
        season_year,
        away_team                                                           AS team,
        COUNT(*)                                                            AS away_games,
        SUM(CASE WHEN winning_team = away_team THEN 1 ELSE 0 END)          AS away_wins,
        SUM(away_runs)                                                      AS away_runs_scored,
        SUM(home_runs)                                                      AS away_runs_conceded
    FROM {{ ref('fct_games') }}
    GROUP BY 1, 2
),

combined AS (
    SELECT
        h.season_year,
        h.team,
        h.home_games,
        a.away_games,
        (h.home_games + a.away_games)                                       AS total_games,
        (h.home_wins + a.away_wins)                                         AS total_wins,
        (h.home_games + a.away_games) - (h.home_wins + a.away_wins)        AS total_losses,
        ROUND(
            SAFE_DIVIDE(h.home_wins + a.away_wins,
                        h.home_games + a.away_games),
        3)                                                                  AS win_pct,
        (h.home_runs_scored + a.away_runs_scored)                           AS total_runs_scored,
        (h.home_runs_conceded + a.away_runs_conceded)                       AS total_runs_conceded,
        (h.home_runs_scored + a.away_runs_scored)
            - (h.home_runs_conceded + a.away_runs_conceded)                 AS run_differential,
        h.avg_home_attendance,
        h.total_home_attendance

    FROM home_stats h
    LEFT JOIN away_stats a
        ON h.team = a.team
        AND h.season_year = a.season_year
)

SELECT
    *,
    RANK() OVER (
        PARTITION BY season_year
        ORDER BY win_pct DESC
    )                                                                       AS season_rank

FROM combined
