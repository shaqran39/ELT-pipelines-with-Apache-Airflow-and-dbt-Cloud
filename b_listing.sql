{{ 
    config(
        unique_key='LISTING_ID',
        alias='listing'
    ) 
}}

select * from {{ source('raw', 'raw_listing') }}
