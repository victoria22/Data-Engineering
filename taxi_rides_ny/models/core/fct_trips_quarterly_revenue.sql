{{
    config(
        materialized='table'
    )
}}

WITH quarterly_revenue AS (
    SELECT 
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,
        SUM(total_amount) AS quarterly_revenue
    FROM {{ ref('fact_trips') }}
    GROUP BY service_type, year, quarter
), 

quarterly_yoy_growth AS (
    SELECT 
        q1.service_type,
        q1.year,
        q1.quarter,
        q1.quarterly_revenue,
        q2.quarterly_revenue AS previous_year_revenue,
        ROUND(
            (q1.quarterly_revenue - q2.quarterly_revenue) / NULLIF(q2.quarterly_revenue, 0) * 100, 
            2
        ) AS yoy_growth_percentage
    FROM quarterly_revenue q1
    LEFT JOIN quarterly_revenue q2 
        ON q1.service_type = q2.service_type 
        AND q1.quarter = q2.quarter
        AND q1.year = q2.year + 1
) 

SELECT * FROM quarterly_yoy_growth
ORDER BY service_type, year, quarter