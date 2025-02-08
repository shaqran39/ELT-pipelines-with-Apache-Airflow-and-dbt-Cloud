-- models/gold/g_fact_listings.sql
{{ 
    config(
        materialized='table',
        schema='gold'
    ) 
}}

with

source as (
    select * from {{ ref('listing_snapshot') }}
),

cleaned as (
    select
        l.listing_id::bigint,
        l.scrape_id::bigint,
        l.scraped_date::timestamp,
        l.host_id::bigint,
        l.host_neighbourhood::text,
        l.listing_neighbourhood::text,
        p.prop_id::bigint,  -- Foreign key to dim_property
        p.property_type::text,
        p.room_type::text,
        p.accommodates::int,
        l.review_scores_rating::int,
        l.price::numeric,
        l.has_availability::boolean,
        l.availability_30::int,
        l.host_is_superhost::boolean,  -- Now sourced directly from listing_snapshot
        r.review_id::bigint,  -- Foreign key to dim_review
        s.dbt_scd_id::bigint, -- Foreign key to dim_scd
        case 
            when l.dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp
            else l.dbt_valid_from
        end as valid_from,
        l.dbt_valid_to as valid_to
    from source l
    left join {{ ref('g_dim_property') }} p 
        on l.property_type = p.property_type
        and l.room_type = p.room_type
        and l.accommodates = p.accommodates
    left join {{ ref('g_dim_review') }} r 
        on l.listing_id = r.review_id
    left join {{ ref('g_dim_scd') }} s 
        on l.listing_id = s.dbt_scd_id
),

unknown as (
    select
        0::bigint as listing_id,
        null::bigint as scrape_id,
        null::timestamp as scraped_date,
        0::bigint as host_id,
        'unknown'::text as host_neighbourhood,
        'unknown'::text as listing_neighbourhood,
        0::bigint as prop_id,
        'unknown'::text as property_type,
        'unknown'::text as room_type,
        0::int as accommodates,
        0::int as review_scores_rating,
        0::numeric as price,
        false::boolean as has_availability,
        0::int as availability_30,
        false::boolean as host_is_superhost,  -- Set default for unknown records
        0::bigint as review_id,
        0::bigint as dbt_scd_id,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as valid_to
)

select * from unknown
union all
select * from cleaned
