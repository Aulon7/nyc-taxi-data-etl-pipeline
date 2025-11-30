{{ config(materialized='table') }}

with ratecodes as (
    select distinct
        ratecode_id,
        case ratecode_id
            when 1 then 'Standard rate'
            when 2 then 'JFK'
            when 3 then 'Newark'
            when 4 then 'Nassau or Westchester'
            when 5 then 'Negotiated fare'
            when 6 then 'Group ride'
            else 'Unknown'
        end as ratecode_description
    from {{ ref('stg_tripsdata_raw') }}
)

select * from ratecodes