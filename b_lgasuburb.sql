{{ 
    config(
        unique_key='suburb_name' ,
        alias='lgasuburb'
    ) 
}}

select * from {{ source('raw', 'raw_lgasuburb') }}
