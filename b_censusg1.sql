{{ 
    config(
        unique_key='lga_code_2016' ,
        alias='censusg1'
    ) 
}}

select * from {{ source('raw', 'raw_censusg1') }}
