{{
    config(
        materialized='table'
    )
}}

WITH filtered_trips_data AS (
    SELECT
        service_type,
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        fare_amount,
        trip_distance,
        payment_type_description
    FROM {{ ref('fact_trips') }}  -- Reference your source table, adjust the name as necessary
    WHERE
        fare_amount > 0
        AND trip_distance > 0
        AND payment_type_description IN ('Cash', 'Credit card')
),
percentile_buckets AS (
    SELECT
        service_type,
        year,
        month,
        fare_amount,
        NTILE(100) OVER (PARTITION BY service_type, year, month ORDER BY fare_amount) AS percentile_rank
    FROM filtered_trips_data
),
percentiles AS (
    SELECT
        service_type,
        year,
        month,
        MAX(CASE WHEN percentile_rank <= 97 THEN fare_amount END) AS p97,
        MAX(CASE WHEN percentile_rank <= 95 THEN fare_amount END) AS p95,
        MAX(CASE WHEN percentile_rank <= 90 THEN fare_amount END) AS p90
    FROM percentile_buckets
    GROUP BY service_type, year, month
)
SELECT
    service_type,
    year,
    month,
    p97,
    p95,
    p90
FROM percentiles