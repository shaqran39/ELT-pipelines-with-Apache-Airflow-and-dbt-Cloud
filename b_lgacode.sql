{{ 
    config(
        unique_key='lga_code' ,
        alias='lgacode'
    ) 
}}

select * from {{ source('raw', 'raw_lgacode') }}
