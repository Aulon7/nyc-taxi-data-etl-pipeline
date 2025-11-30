{{ config(materialized='view') }}

with source as (
    select
        zone_id,
        zone_name,
        borough,
        zone_geom
    from `bigquery-public-data`.new_york_taxi_trips.taxi_zone_geom
)

select * from source