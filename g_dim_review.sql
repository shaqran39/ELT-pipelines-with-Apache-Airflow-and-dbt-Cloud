-- models/gold/star/g_dim_review.sql
{{
    config(
        unique_key='review_id',
        alias='dim_review'
    )
}}

with

source as (
    select * from {{ ref('listing_snapshot') }}  -- Reference to the snapshot table
),

cleaned as (
    select
        row_number() over () as review_id,  -- Generate review_id as a surrogate key
        number_of_reviews,
        review_scores_rating,
        review_scores_accuracy,
        review_scores_cleanliness,
        review_scores_checkin,
        review_scores_communication,
        review_scores_value,
        case
            when dbt_valid_from = (select min(dbt_valid_from) from source) then '1900-01-01'::timestamp
            else dbt_valid_from
        end as valid_from,
        dbt_valid_to as valid_to
    from source
    group by number_of_reviews, review_scores_rating, review_scores_accuracy, 
             review_scores_cleanliness, review_scores_checkin, review_scores_communication,
             review_scores_value, dbt_valid_from, dbt_valid_to
),

unknown as (
    select
        0 as review_id,
        0 as number_of_reviews,
        0 as review_scores_rating,
        0 as review_scores_accuracy,
        0 as review_scores_cleanliness,
        0 as review_scores_checkin,
        0 as review_scores_communication,
        0 as review_scores_value,
        '1900-01-01'::timestamp as valid_from,
        null::timestamp as valid_to
)

select * from unknown
union all
select * from cleaned
