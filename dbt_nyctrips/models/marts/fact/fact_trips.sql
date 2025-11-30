{{ config(materialized='table') }}

select
    vendor_id,
    pickup_datetime,
    dropoff_datetime,   
    cast(pickup_datetime as date) as pickup_date,
    passenger_count,
    trip_distance,
    ratecode_id,
    store_and_fwd_flag,
    pu_location_id,
    do_location_id,
    payment_type,
    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    total_amount,
    congestion_surcharge,
    airport_fee
from {{ ref('stg_tripsdata_raw') }}