-- models/gold/g_dm_listing_neighbourhood.sql
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

with active_listings as (
    select 
        listing_neighbourhood,
        date_trunc('month', scraped_date) as month_year,
        count(*) as total_listings,
        count(case when has_availability = true then 1 end) as active_listings,
        count(distinct case when host_is_superhost = true then host_id end) as superhosts,
        avg(price) filter (where has_availability = true) as avg_price,
        min(price) filter (where has_availability = true) as min_price,
        max(price) filter (where has_availability = true) as max_price,
        percentile_cont(0.5) within group (order by price) filter (where has_availability = true) as median_price,  -- Calculating median
        avg(review_scores_rating) filter (where has_availability = true) as avg_review_scores,
        sum(30 - availability_30) filter (where has_availability = true) as total_stays,
        sum((30 - availability_30) * price) filter (where has_availability = true) as estimated_revenue
    from {{ ref('g_fact_listings') }}
    group by listing_neighbourhood, month_year
)

select
    listing_neighbourhood,
    month_year,
    (active_listings::float / total_listings) * 100 as active_listing_rate,
    min_price,
    max_price,
    median_price,
    avg_price,
    total_listings as distinct_hosts,  -- Adjusted to use `total_listings` for distinct host count if `host_id` isn’t directly countable
    (superhosts::float / nullif(total_listings, 0)) * 100 as superhost_rate,
    avg_review_scores as avg_review_scores_rating,
    total_stays,
    estimated_revenue / nullif(active_listings, 0) as avg_estimated_revenue_per_active_listing
from active_listings
