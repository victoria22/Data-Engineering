{{
    config(
        materialized='table'
    )
}}

with trips as (
    select
        extract(year from pickup_datetime) as year,
        extract(month from pickup_datetime) as month,
        PUlocationID,
        DOlocationID,
        pickup_zone,
        dropoff_zone,
        timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips') }}
    WHERE pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
        AND EXTRACT(YEAR FROM pickup_datetime) = 2019
        AND EXTRACT(MONTH FROM pickup_datetime) = 11
),

ranked_trips AS (
    SELECT
        year,
        month,
        PUlocationID,
        DOlocationID,
        pickup_zone,
        dropoff_zone,
        trip_duration,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, PUlocationID, DOlocationID
            ORDER BY trip_duration DESC
        ) AS trip_rank
    FROM trips
),
p90_trips AS (
    SELECT
        year,
        month,
        PUlocationID,
        DOlocationID,
        pickup_zone,
        dropoff_zone,
        trip_duration
    FROM ranked_trips
    WHERE trip_rank <= (SELECT CEIL(COUNT(*) * 0.1)  -- Calculate the top 10% trips
                         FROM ranked_trips t2 
                         WHERE t2.year = ranked_trips.year 
                         AND t2.month = ranked_trips.month 
                         AND t2.PUlocationID = ranked_trips.PUlocationID 
                         AND t2.DOlocationID = ranked_trips.DOlocationID)
),
ranked_p90_trips AS (
    SELECT
        year,
        month,
        PUlocationID,
        DOlocationID,
        pickup_zone,
        dropoff_zone,
        trip_duration,
        ROW_NUMBER() OVER (
            PARTITION BY year, month, PUlocationID
            ORDER BY trip_duration DESC
        ) AS p90_rank
    FROM p90_trips
)
SELECT
    year,
    month,
    PUlocationID,
    DOlocationID,
    pickup_zone,
    dropoff_zone,
    trip_duration
FROM ranked_p90_trips
WHERE p90_rank = 2  -- Select the 2nd longest p90 trip duration for each pickup zone
  AND pickup_zone IN ('Newark Airport', 'SoHo', 'Yorkville East')
ORDER BY pickup_zone