------Question A

WITH LGA_Revenue AS (
    SELECT 
        lc.LGA_CODE,
        AVG(r.PRICE * r.AVAILABILITY_30) * 12 AS estimated_revenue
    FROM 
        dbt_ssaleh_silver.s_listing r
    JOIN 
        dbt_ssaleh_silver.s_lgasuburb ls ON r.LISTING_NEIGHBOURHOOD = ls.SUBURB_NAME
    JOIN 
        dbt_ssaleh_silver.s_lgacode lc ON ls.LGA_NAME = lc.LGA_NAME
    GROUP BY 
        lc.LGA_CODE
),
Top_Lowest_LGAs AS (
    SELECT 
        LGA_CODE,
        estimated_revenue,
        RANK() OVER (ORDER BY estimated_revenue DESC) AS rank_high,
        RANK() OVER (ORDER BY estimated_revenue ASC) AS rank_low
    FROM 
        LGA_Revenue
)
SELECT 
    l.LGA_CODE,
    c1.Tot_P_P AS total_population,
    c2.Median_age_persons,
    c2.Average_household_size
FROM 
    Top_Lowest_LGAs l
JOIN 
    dbt_ssaleh_silver.s_censusg1 c1 ON l.LGA_CODE::VARCHAR = c1.LGA_CODE_2016
JOIN 
    dbt_ssaleh_silver.s_cemsusg2 c2 ON l.LGA_CODE::VARCHAR = c2.LGA_CODE_2016
WHERE 
    rank_high <= 3 OR rank_low <= 3
ORDER BY 
    rank_high, rank_low;

-----------------------------------------------------------------------------------------

------Question B

WITH Revenue_Per_LGA AS (
    SELECT 
        lc.LGA_CODE,
        AVG(r.PRICE * r.AVAILABILITY_30) * 12 AS estimated_revenue
    FROM 
        bronze.raw_listing r
    JOIN 
        bronze.raw_lgasuburb ls ON r.LISTING_NEIGHBOURHOOD = ls.SUBURB_NAME
    JOIN 
        bronze.raw_lgacode lc ON ls.LGA_NAME = lc.LGA_NAME
    GROUP BY 
        lc.LGA_CODE
)
SELECT 
    r.estimated_revenue,
    c.Median_age_persons
FROM 
    Revenue_Per_LGA r
JOIN 
    bronze.raw_censusg2 c ON r.LGA_CODE = c.LGA_CODE_2016::INTEGER;

-------------------------------------------------------------------------------------

------Question C

WITH Revenue_Per_LGA AS (
    SELECT 
        lc.LGA_CODE,
        AVG(r.PRICE * r.AVAILABILITY_30) * 12 AS estimated_revenue
    FROM 
        bronze.raw_listing r
    JOIN 
        bronze.raw_lgasuburb ls ON r.LISTING_NEIGHBOURHOOD = ls.SUBURB_NAME
    JOIN 
        bronze.raw_lgacode lc ON ls.LGA_NAME = lc.LGA_NAME
    GROUP BY 
        lc.LGA_CODE
)
SELECT 
    r.estimated_revenue,
    c.Median_age_persons
FROM 
    Revenue_Per_LGA r
JOIN 
    bronze.raw_censusg2 c ON r.LGA_CODE = c.LGA_CODE_2016::INTEGER;
   
   
   WITH Neighbourhood_Revenue AS (
    SELECT 
        LISTING_NEIGHBOURHOOD,
        AVG(PRICE * AVAILABILITY_30) * 12 AS estimated_revenue
    FROM 
        bronze.raw_listing
    GROUP BY 
        LISTING_NEIGHBOURHOOD
),
Top_Neighbourhoods AS (
    SELECT 
        LISTING_NEIGHBOURHOOD
    FROM 
        Neighbourhood_Revenue
    ORDER BY 
        estimated_revenue DESC
    LIMIT 5
)
SELECT 
    r.LISTING_NEIGHBOURHOOD,
    r.PROPERTY_TYPE,
    r.ROOM_TYPE,
    r.ACCOMMODATES,
    COUNT(*) AS stay_count
FROM 
    bronze.raw_listing r
JOIN 
    Top_Neighbourhoods t ON r.LISTING_NEIGHBOURHOOD = t.LISTING_NEIGHBOURHOOD
GROUP BY 
    r.LISTING_NEIGHBOURHOOD, r.PROPERTY_TYPE, r.ROOM_TYPE, r.ACCOMMODATES
ORDER BY 
    stay_count DESC
LIMIT 5;


--------------------------------------------------------------------------
------Question D


WITH Single_Hosts AS (
    SELECT 
        HOST_ID,
        LISTING_ID,
        lc.LGA_CODE,
        PRICE * AVAILABILITY_30 * 12 AS estimated_revenue
    FROM 
        bronze.raw_listing r
    JOIN 
        bronze.raw_lgasuburb ls ON r.LISTING_NEIGHBOURHOOD = ls.SUBURB_NAME
    JOIN 
        bronze.raw_lgacode lc ON ls.LGA_NAME = lc.LGA_NAME
    GROUP BY 
        HOST_ID, LISTING_ID, lc.LGA_CODE, PRICE, AVAILABILITY_30
    HAVING 
        COUNT(*) = 1
),
Mortgage_Coverage AS (
    SELECT 
        s.HOST_ID,
        s.LGA_CODE,
        s.estimated_revenue,
        c.Median_mortgage_repay_monthly * 12 AS annual_mortgage,
        CASE 
            WHEN s.estimated_revenue >= (c.Median_mortgage_repay_monthly * 12) THEN 1
            ELSE 0
        END AS covers_mortgage
    FROM 
        Single_Hosts s
    JOIN 
        bronze.raw_censusg2 c ON s.LGA_CODE = CAST(REGEXP_REPLACE(c.LGA_CODE_2016, '[^0-9]', '', 'g') AS INTEGER)
)
SELECT 
    LGA_CODE,
    AVG(covers_mortgage) * 100 AS percentage_covering_mortgage
FROM 
    Mortgage_Coverage
GROUP BY 
    LGA_CODE
ORDER BY 
    percentage_covering_mortgage DESC
LIMIT 1;


----------------------------------------------------
------Question E

WITH single_listing_hosts AS (
    SELECT 
        host_id,
        host_neighbourhood AS lga_code,
        SUM(price * (30 - availability_30)) AS annual_revenue
    FROM s_listing
    GROUP BY host_id, host_neighbourhood
    HAVING COUNT(listing_id) = 1
),
mortgage_coverage AS (
    SELECT 
        single.host_id,
        single.lga_code,
        single.annual_revenue,
        census.median_mortgage_repay_monthly * 12 AS annual_mortgage_repay,
        CASE 
            WHEN single.annual_revenue >= census.median_mortgage_repay_monthly * 12 
            THEN 'Yes' ELSE 'No' END AS covers_mortgage
    FROM single_listing_hosts AS single
    JOIN s_censusg2 AS census
    ON single.lga_code = census.lga_code_2016
)
SELECT 
    lga_code,
    SUM(CASE WHEN covers_mortgage = 'Yes' THEN 1 ELSE 0 END)::float / COUNT(*) * 100 AS coverage_percentage
FROM mortgage_coverage
GROUP BY lga_code
ORDER BY coverage_percentage DESC
LIMIT 1;



