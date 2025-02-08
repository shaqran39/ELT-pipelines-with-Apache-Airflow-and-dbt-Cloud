{{ 
    config(
        materialized='table',
        schema='silver'
    ) 
}}

select
    trim(LGA_NAME) as lga_name,  -- Removing any leading/trailing spaces
    trim(SUBURB_NAME) as suburb_name
from {{ ref('b_lgasuburb') }}  -- Pull data from the bronze model
