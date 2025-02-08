-- models/gold/g_dm_host_neighbourhood.sql
{{
    config(
        materialized='view',
        schema='gold'
    )
}}

with active_listings as (
    select 
        host_neighbourhood as host_neighbourhood_lga,
        date_trunc('month', scraped_date) as month_year,
        count(distinct host_id) as distinct_hosts,
        sum((30 - availability_30) * price) filter (where has_availability = true) as estimated_revenue
    from {{ ref('g_fact_listings') }}
    group by host_neighbourhood, month_year
)

select
    host_neighbourhood_lga,
    month_year,
    distinct_hosts,
    estimated_revenue,
    estimated_revenue / nullif(distinct_hosts, 0) as estimated_revenue_per_host
from active_listings
