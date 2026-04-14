{{
  config(
    materialized='table',
    tags=['static']
  )
}}



SELECT
    team_id,
    team_name,
    is_redsox,

    -- Division mapping (AL East highlighted for RedSox context)
    CASE
        WHEN team_name IN ('Red Sox', 'Yankees',
                           'Blue Jays', 'Rays',
                           'Orioles')               THEN 'AL East'
        WHEN team_name IN ('White Sox', 'Indians',
                           'Tigers', 'Royals',
                           'Twins')                 THEN 'AL Central'
        WHEN team_name IN ('Astros', 'Angels',
                           'Athletics', 'Mariners',
                           'Rangers')                   THEN 'AL West'
        WHEN team_name IN ('Braves', 'Marlins',
                           'Mets', 'Phillies',
                           'Nationals')            THEN 'NL East'
        WHEN team_name IN ('Cubs', 'Reds',
                           'Brewers', 'Pirates',
                           'Cardinals')             THEN 'NL Central'
        WHEN team_name IN ('Diamondbacks', 'Rockies',
                           'Dodgers', 'Padres',
                           'Giants')            THEN 'NL West'
        ELSE 'Unknown'
    END                                                       AS division,

    CASE
        WHEN team_name IN ('Red Sox', 'Yankees',
                           'Blue Jays', 'Rays',
                           'Orioles', 'White Sox',
                           'Indians', 'Tigers', 'Royals',
                           'Twins', 'Astros', 'Angels',
                           'Athletics', 'Mariners', 'Rangers')                   THEN 'AL'
        ELSE 'NL'
    END                                                       AS league

FROM {{ ref('stg_teams') }}
