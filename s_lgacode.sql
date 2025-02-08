{{ 
    config(
        materialized='table',
        schema='silver'
    ) 
}}

select
    LGA_CODE,
    LGA_NAME
from {{ ref('b_lgacode') }}  -- Pull data from the bronze model
