{{ config(materialized='table') }}

with dates as (
    select distinct
        date(pickup_datetime) as date_day
    from {{ ref('stg_tripsdata_raw') }}
),

expanded_dates as (
    select
        date_day as date_id,
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(dayofweek from date_day) as weekday,
        case when extract(dayofweek from date_day) in (1, 7)
            then true else false end as is_weekend
    from dates
)

select * from expanded_dates