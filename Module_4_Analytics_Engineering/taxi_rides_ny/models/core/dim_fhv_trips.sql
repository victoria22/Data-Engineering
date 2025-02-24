{{
    config(
        materialized='table'
    )
}}

with fhv_tripdata as (
    select *
    from {{ ref('stg_fhv_tripdata') }}
), 

dim_zones as (
    select * 
    from {{ ref('dim_zones') }}
    where borough != 'Unknown'
)

select
    EXTRACT(YEAR FROM pickup_datetime) AS year,
    EXTRACT(MONTH FROM pickup_datetime) AS month,
    dispatching_base_num,
    pickup_datetime,
    dropOff_datetime,
    PUlocationID,
    DOlocationID,
    SR_Flag,
    Affiliated_base_number,
    pickup_zone.borough AS pickup_borough, 
    pickup_zone.zone AS pickup_zone,
    dropoff_zone.borough AS dropoff_borough, 
    dropoff_zone.zone AS dropoff_zone,
    TIMESTAMP_DIFF(dropOff_datetime, pickup_datetime, SECOND) AS trip_duration  -- Calculating trip duration in seconds

from fhv_tripdata
inner join dim_zones AS pickup_zone
    on fhv_tripdata.PUlocationID = pickup_zone.locationid
inner join dim_zones AS dropoff_zone
    on fhv_tripdata.DOlocationID = dropoff_zone.locationid
