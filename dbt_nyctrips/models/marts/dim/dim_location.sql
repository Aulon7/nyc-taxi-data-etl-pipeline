{{ config(materialized='table') }}

with all_locations as (
    select pu_location_id as location_id
    from {{ ref('stg_tripsdata_raw') }}
    where pu_location_id is not null AND pu_location_id != 0
    union all
    select do_location_id as location_id
    from {{ ref('stg_tripsdata_raw') }}
    where do_location_id is not null AND do_location_id != 0
),

unique_trip_ids as (
    select distinct location_id
    from all_locations
),

cleaned_zone_data as (
    select
        safe_cast(zone_id as int64) as location_id,
        zone_name,
        borough,
        zone_geom,
        row_number() over (partition by zone_id order by zone_name) as rn 
    from {{ ref('stg_taxi_zone_geom') }}
),

joined as (
    select
        l.location_id,
        coalesce(z.zone_name, 'UNKNOWN') as zone_name,
        coalesce(z.borough, 'UNKNOWN') as borough,
        z.zone_geom
    from unique_trip_ids l
    left join cleaned_zone_data z
        on l.location_id = z.location_id
    where z.rn = 1 or z.rn is null
)
select
    location_id,
    zone_name,
    borough,
    zone_geom
from joined