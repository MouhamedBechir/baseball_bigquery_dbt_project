{{
  config(
    materialized='table',
    partition_by={
      "field": "startTime",
      "data_type": "timestamp",
      "granularity": "month"
    },
    cluster_by=["homeTeamName", "awayTeamName"]
  )
}}

SELECT *
FROM {{ source('baseball_public', 'games_post_wide') }}
