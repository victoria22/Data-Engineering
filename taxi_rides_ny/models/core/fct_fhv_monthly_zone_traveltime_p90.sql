{{
    config(
        materialized='table'
    )
}}

WITH trips AS (
    SELECT
        EXTRACT(YEAR FROM pickup_datetime) AS year,
        EXTRACT(MONTH FROM pickup_datetime) AS month,
        PUlocationID,
        DOlocationID,
        pickup_zone,
        dropoff_zone,
        TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, SECOND) AS trip_duration
    FROM {{ ref('dim_fhv_trips') }}
    WHERE pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
        AND EXTRACT(YEAR FROM pickup_datetime) = 2019
        AND EXTRACT(MONTH FROM pickup_datetime) = 11
),
trip_p90 AS (
    SELECT
        year,
        month,
        PUlocationID,
        DOlocationID,
        pickup_zone,
        dropoff_zone,
        APPROX_QUANTILES(trip_duration, 100)[OFFSET(90)] AS p90_trip_duration
    FROM trips
    GROUP BY year, month, PUlocationID, DOlocationID, pickup_zone, dropoff_zone
),
ranked_trips AS (
    SELECT
        pickup_zone,
        dropoff_zone,
        p90_trip_duration,
        DENSE_RANK() OVER (PARTITION BY pickup_zone ORDER BY p90_trip_duration DESC) AS rnk
    FROM trip_p90
),
final_result AS (
    -- Get the 2nd highest P90 trip duration if it exists
    SELECT pickup_zone, dropoff_zone, p90_trip_duration
    FROM ranked_trips
    WHERE rnk = 2

    UNION ALL

    -- If no 2nd highest P90 exists, return the longest (fallback)
    SELECT pickup_zone, dropoff_zone, p90_trip_duration
    FROM ranked_trips
    WHERE rnk = 1
    AND pickup_zone NOT IN (SELECT DISTINCT pickup_zone FROM ranked_trips WHERE rnk = 2)
)
SELECT * FROM final_result
