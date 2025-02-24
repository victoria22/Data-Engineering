# Module 4 Homework: Analytics Engineering with DBT and BigQuery

## Question 5: Taxi Quarterly Revenue Growth
### Considering the YoY Growth in 2020, which were the yearly quarters with the best (or less worse) and worst results for green, and yellow. The model has already been built and stored in Bigquery. It can be found in the taxi_rides_ny folder called fct_taxi_trips_quarterly_revenue.sql

### Best Performing Quarter
<pre>WITH Rank_QuarterlyYoY AS (
    SELECT
        service_type,          
        YEAR,
        Quarter,
        yoy_growth_percentage,         
        RANK() OVER (PARTITION BY service_type ORDER BY yoy_growth_percentage DESC) AS YOY_Rank
    FROM
        `dtc-de-course-448012.dbt_vanaeronweke.fct_taxi_trips_quarterly_revenue` 
    WHERE
        YEAR = 2020
)
SELECT
    service_type,
    quarter,
    yoy_growth_percentage
FROM
    Rank_QuarterlyYoY
WHERE
    yoy_rank = 1 -- selecting the top performing quarter(s)
ORDER BY
    service_type, quarter; </pre> 

### Worst Performing Quarter
<pre>WITH Rank_QuarterlyYoY AS (
    SELECT
        service_type,          
        YEAR,
        Quarter,
        yoy_growth_percentage,         
        RANK() OVER (PARTITION BY service_type ORDER BY yoy_growth_percentage) AS YOY_Rank
    FROM
        `dtc-de-course-448012.dbt_vanaeronweke.fct_taxi_trips_quarterly_revenue` 
    WHERE
        YEAR = 2020
)
SELECT
    service_type,
    quarter,
    yoy_growth_percentage
FROM
    Rank_QuarterlyYoY
WHERE
    yoy_rank = 1 -- selecting the worst performing quarter(s)
ORDER BY
    service_type, quarter; </pre>

## Question 6: P97/P95/P90 Taxi Monthly Fare
### The model and continuous percentile has already been built and computed in dbt, and stored in bigquery. The model can be found in the taxi_rides_ny folder called fct_taxi_trips_monthly_fare_p95.sql
<pre>SELECT
  service_type,
  year,
  month,
  p97,
  p95,
  p90
FROM `dtc-de-course-448012.dbt_vanaeronweke.fct_taxi_trips_monthly_fare_p95` </pre>

## Question 7: Top #Nth longest P90 travel time Location for FHV
### The model and continuous p90 of trip_duration has already been built and computed in dbt and stored in bigquery. The model can be found in the taxi_rides_ny folder called fct_fhv_monthly_zone_traveltime_p90.sql
<pre>SELECT 
  pickup_zone,
  dropoff_zone,
  p90_trip_duration
FROM `dtc-de-course-448012.dbt_vanaeronweke.fct_fhv_monthly_zone_traveltime_p90` </pre>