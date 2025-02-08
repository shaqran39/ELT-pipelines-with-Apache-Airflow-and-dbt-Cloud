-- models/gold/star/g_dim_property.sql
{{
    config(
        unique_key='prop_id',
        alias='dim_property'
    )
}}

with

source as (
    select * from {{ ref('listing_snapshot') }}  -- Reference to the snapshot table
),

cleaned as (
    select
        row_number() over () as prop_id,  -- Generate prop_id as a surrogate key
        property_type,
        room_type,
        accommodates,
        case
            when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp
            else dbt_valid_from
        end as valid_from,
        dbt_valid_to as valid_to
    from source
    group by property_type, room_type, accommodates, dbt_valid_from, dbt_valid_to
),

unknown as (
    select
        0 as prop_id,
        'unknown' as property_type,
        'unknown' as room_type,
        0 as accommodates,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as valid_to
)

select * from unknown
union all
select * from cleaned
