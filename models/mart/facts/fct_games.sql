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
    -- Keys
    g.game_id,
    g.game_date,
    g.season_year,
    g.season_month,

    -- Teams
    g.home_team,
    g.away_team,
    ht.is_redsox                        AS home_is_redsox,
    at.is_redsox                        AS away_is_redsox,
    ht.division                         AS home_team_division,
    at.division                         AS away_team_division,

    -- Scores
    g.home_runs,
    g.away_runs,
    g.total_runs,
    g.run_differential,

    -- Outcome
    g.winning_team,
    g.win_type,

    -- Venue
    g.venue_id,
    g.venue_name,

    -- Game metadata
    g.attendance,
    g.durationMinutes,

    -- Game type
    'REGULAR'                           AS game_type,

    -- Audit
    CURRENT_TIMESTAMP()                 AS loaded_at

FROM {{ ref('stg_games') }} g
LEFT JOIN {{ ref('dim_teams') }} ht
    ON g.home_team = ht.team_name
LEFT JOIN {{ ref('dim_teams') }} at
    ON g.away_team = at.team_name

{% if is_incremental() %}
    WHERE g.game_date > (
        SELECT MAX(game_date)
        FROM {{ this }}
    )
{% endif %}
