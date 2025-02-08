-- models/silver/s_listing.sql
{{ 
    config(
        materialized='table',
        schema='silver'
    ) 
}}

with cleaned_data as (
    select
        LISTING_ID,
        SCRAPE_ID,
        cast(SCRAPED_DATE as date) as scraped_date,  -- Cast directly (YYYY-MM-DD format)
        HOST_ID,
        COALESCE(HOST_NAME, '') as HOST_NAME,  -- Remove nulls, replace with empty string
        
        -- Check if HOST_SINCE is a valid date, if not, set it to NULL
        case
            when HOST_SINCE ~ '^[0-9]{1,2}/[0-9]{1,2}/[0-9]{4}$' -- Regular expression to match a valid DD/MM/YYYY format
            then to_date(HOST_SINCE, 'DD/MM/YYYY')
            else null
        end as host_since,  -- Convert using the correct format (DD/MM/YYYY)
        
        case 
            when HOST_IS_SUPERHOST = 't' then true 
            when HOST_IS_SUPERHOST = 'f' then false
            else false  -- Remove nulls, replace with false
        end as host_is_superhost,  -- Convert to boolean
        
        upper(COALESCE(HOST_NEIGHBOURHOOD, '')) as HOST_NEIGHBOURHOOD,  -- Remove nulls and convert to uppercase
        upper(COALESCE(LISTING_NEIGHBOURHOOD, '')) as LISTING_NEIGHBOURHOOD,  -- Remove nulls and convert to uppercase
        COALESCE(PROPERTY_TYPE, '') as PROPERTY_TYPE,  -- Remove nulls
        COALESCE(ROOM_TYPE, '') as ROOM_TYPE,  -- Remove nulls
        COALESCE(ACCOMMODATES, 0) as ACCOMMODATES,  -- Remove nulls, replace with 0
        COALESCE(PRICE, 0) as PRICE,  -- Remove nulls, replace with 0
        
        case 
            when HAS_AVAILABILITY = 't' then true 
            when HAS_AVAILABILITY = 'f' then false
            else false  -- Remove nulls, replace with false
        end as has_availability,  -- Convert to boolean
        
        COALESCE(AVAILABILITY_30, 0) as AVAILABILITY_30,  -- Remove nulls, replace with 0
        COALESCE(NUMBER_OF_REVIEWS, 0) as NUMBER_OF_REVIEWS,  -- Remove nulls, replace with 0
        COALESCE(REVIEW_SCORES_RATING, 0) as REVIEW_SCORES_RATING,  -- Remove nulls, replace with 0
        COALESCE(REVIEW_SCORES_ACCURACY, 0) as REVIEW_SCORES_ACCURACY,  -- Remove nulls, replace with 0
        COALESCE(REVIEW_SCORES_CLEANLINESS, 0) as REVIEW_SCORES_CLEANLINESS,  -- Remove nulls, replace with 0
        COALESCE(REVIEW_SCORES_CHECKIN, 0) as REVIEW_SCORES_CHECKIN,  -- Remove nulls, replace with 0
        COALESCE(REVIEW_SCORES_COMMUNICATION, 0) as REVIEW_SCORES_COMMUNICATION,  -- Remove nulls, replace with 0
        COALESCE(REVIEW_SCORES_VALUE, 0) as REVIEW_SCORES_VALUE  -- Remove nulls, replace with 0
    from {{ ref('b_listing') }}  -- Pull data from the bronze model
)

select * from cleaned_data
