{{
    config(
        materialized='table'
    )
}}

WITH quarterly_revenue AS (
    SELECT 
        service_type,
        EXTRACT(QUARTER FROM pickup_datetime) AS quarter,  -- Extracts quarter number (1,2,3,4)
        EXTRACT(YEAR FROM pickup_datetime) AS year,        -- Extracts year number
        SUM(total_amount) AS quarterly_revenue
    FROM {{ ref('fact_trips') }}
    GROUP BY service_type, quarter, year
),

quarterly_growth AS (
    SELECT 
        q1.service_type,
        q1.year,
        q1.quarter,
        q1.quarterly_revenue,
        q2.quarterly_revenue AS previous_year_revenue,
        ROUND(
            (q1.quarterly_revenue - q2.quarterly_revenue) / NULLIF(q2.quarterly_revenue, 0) * 100, 2
        ) AS yoy_growth_percentage
    FROM quarterly_revenue q1
    LEFT JOIN quarterly_revenue q2
        ON q1.service_type = q2.service_type  -- Compare within the same service type
        AND q1.quarter = q2.quarter           -- Same quarter
        AND q1.year = q2.year + 1             -- Compare with previous year
    WHERE q1.year = 2020

),

ranked_quarters AS (
    SELECT 
        service_type,
        year,
        quarter,
        yoy_growth_percentage,
        RANK() OVER (PARTITION BY service_type ORDER BY yoy_growth_percentage DESC) AS best_rank,
        RANK() OVER (PARTITION BY service_type ORDER BY yoy_growth_percentage ASC) AS worst_rank
    FROM quarterly_growth
)

SELECT 
    service_type,
    year,
    quarter AS best_quarter,
    yoy_growth_percentage AS best_growth
FROM ranked_quarters
WHERE best_rank = 1

UNION ALL

SELECT 
    service_type,
    year,
    quarter AS worst_quarter,
    yoy_growth_percentage AS worst_growth
FROM ranked_quarters
WHERE worst_rank = 1
ORDER BY service_type, best_growth DESC
