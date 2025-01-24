# Module 1 Homework: Docker & SQL

## Question 1
Code for running python:3.12.8 image in an interactive mode in docker is:
`docker run -it --entrypoint /bin/bash python:3.12.8`
To get the version of run: `pip --version` 

## Question 2
From the docker-compose file, the hostname pgadmin will use to connect to Postgres database is **db**. This is the service name of the postgres container and will resolve the container's internal IP address within the same network. The port number is 5432. This is because **db** is exposing port **5432** inside the container but is mapped to port **5433** on the host machine. Docker compose uses internal network for container to container communication.  
Code to run the docker compose file is : `docker-compose up -d`

## Code to ingest the data from ingest_data.py to postgres database in docker
<pre> docker run -it --network week1assignment_week1assignment-network green_taxi_ingest:v001 \
  postgres postgres db 5432 ny_taxi green_taxi_trips "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz" zones "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv" </pre>

## Question 3. Trip Segmentation Count
### Up to 1 mile
1. SELECT COUNT(trip_distance) AS trip_count
   FROM green_taxi_trips
   WHERE trip_distance <= 1
    AND DATE(lpep_pickup_datetime) >= '2019-10-01'
    AND DATE(lpep_dropoff_datetime) < '2019-11-01';

### In between 1 (exclusive) and 3 miles (inclusive)
2. SELECT COUNT(trip_distance) AS trip_count
   FROM green_taxi_trips
   WHERE trip_distance > 1 
    AND trip_distance <= 3
    AND DATE(lpep_pickup_datetime) >= '2019-10-01'
    AND DATE(lpep_dropoff_datetime) < '2019-11-01';

### In between 3 (exclusive) and 7 miles (inclusive)
3. SELECT COUNT(trip_distance) AS trip_count
   FROM green_taxi_trips
   WHERE trip_distance > 3 
    AND trip_distance <= 7
    AND DATE(lpep_pickup_datetime) >= '2019-10-01'
    AND DATE(lpep_dropoff_datetime) < '2019-11-01';

### In between 7 (exclusive) and 10 miles (inclusive)
4. SELECT COUNT(trip_distance) AS trip_count
   FROM green_taxi_trips
   WHERE trip_distance > 7 
    AND trip_distance <= 10
    AND DATE(lpep_pickup_datetime) >= '2019-10-01'
    AND DATE(lpep_dropoff_datetime) < '2019-11-01';

### Over 10 miles
5. SELECT COUNT(trip_distance) AS trip_count
   FROM green_taxi_trips
   WHERE trip_distance > 10 
    AND DATE(lpep_pickup_datetime) >= '2019-10-01'
    AND DATE(lpep_dropoff_datetime) < '2019-11-01';

## Question 4. Longest trip for each day
SELECT
    DATE(lpep_pickup_datetime) AS trip_date,
    MAX(trip_distance) AS longest_trip_distance
FROM
    green_taxi_trips
GROUP BY
    DATE(lpep_pickup_datetime)
ORDER BY
    longest_trip_distance DESC;

## Question 5. Three biggest pickup zones
SELECT 
    SUM(t.total_amount) AS totalAmount, 
    z."Zone" AS Pickup_Locations
FROM green_taxi_trips t
INNER JOIN zones z on t."PULocationID" = z."LocationID"
WHERE DATE(lpep_pickup_datetime) = '2019-10-18'
GROUP BY Pickup_Locations
HAVING SUM(t.total_amount) >13000
ORDER BY totalAmount DESC;

## Question 6. Largest tip
SELECT 
    MAX(t.tip_amount) AS largest_tip_amount, 
    z."Zone" AS DropOff_Zone
FROM green_taxi_trips t
INNER JOIN zones z on t."DOLocationID" = z."LocationID"
WHERE DATE(lpep_pickup_datetime) = '2019-10-19' AND z."Zone" = 'East Harlem North'
GROUP BY DropOff_Zone

