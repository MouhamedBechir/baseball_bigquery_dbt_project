{{
  config(
    materialized='incremental',
    unique_key='game_id',
    partition_by={
      "field": "game_date",
      "data_type": "date",
      "granularity": "month"
    },
    cluster_by=["home_team", "season_year"],
    on_schema_change='append_new_columns',
    incremental_strategy='merge'
  )
}}

SELECT
    p.game_id,
    p.game_date,
    p.season_year,
    p.home_team,
    p.away_team,
    p.home_runs,
    p.away_runs,
    p.winning_team,
    p.game_type,

    -- Team flags
    ht.is_redsox                        AS home_is_redsox,
    away_teams.is_redsox                 AS away_is_redsox,
    ht.division                         AS home_team_division,
    away_teams.division                  AS away_team_division,

    CURRENT_TIMESTAMP()                 AS loaded_at

FROM {{ ref('stg_postseason') }} p
LEFT JOIN {{ ref('dim_teams') }} ht
    ON p.home_team = ht.team_name
LEFT JOIN {{ ref('dim_teams') }} away_teams
    ON p.away_team = away_teams.team_name

{% if is_incremental() %}
    WHERE p.game_date > (
        SELECT MAX(game_date)
        FROM {{ this }}
    )
{% endif %}
