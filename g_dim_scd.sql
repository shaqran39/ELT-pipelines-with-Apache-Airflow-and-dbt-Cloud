{{
    config(
        unique_key='dbt_scd_id',
        alias='dim_scd',
        materialized='table',
        schema='gold'
    )
}}

with

source as (
    select * from {{ ref('listing_snapshot') }}  -- Assuming you have a snapshot table called 'scd_snapshot'
),

cleaned as (
    select
        listing_id as dbt_scd_id,  -- Surrogate key for SCD
        dbt_updated_at,
        case when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp else dbt_valid_from end as valid_from,
        dbt_valid_to as valid_to
    from source
),

unknown as (
    select
        0 as dbt_scd_id,   -- Unknown SCD record
        null::timestamp as dbt_updated_at,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as valid_to
)

select * from unknown
union all
select * from cleaned
