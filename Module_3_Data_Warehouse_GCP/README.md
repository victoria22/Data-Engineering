# Module 3 Homework: Data-Warehouse With GCP & BIG QUERY

## BIG QUERY SETUP
### Create external table using Yellow Taxi Trip Records
<pre>CREATE OR REPLACE EXTERNAL TABLE `dezoomcamp32.external_yellow_tripdata`
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://dtc_de_zoomcamp_bucket/yellow_tripdata_2024-*.parquet']
); </pre>  

### Create a regular/materialised table in BQ using the External Table
<pre>CREATE OR REPLACE TABLE dezoomcamp32.yellow_tripdata AS
SELECT * FROM dezoomcamp32.external_yellow_tripdata; </pre> 

## Question 1: Count of records for the 2024 Yellow Taxi Data
<pre>SELECT COUNT(*) FROM `dezoomcamp32.yellow_tripdata` </pre>

## Question 2: Count distinct PULocationIDs for the entire dataset and get the estimated amount of data
<pre>SELECT COUNT(DISTINCT(PULocationID)) FROM `dezoomcamp32.external_yellow_tripdata` </pre>  

<pre>SELECT COUNT(DISTINCT(PULocationID)) FROM `dezoomcamp32.yellow_tripdata` </pre>  

## Question 3: Retrieve columns 
<pre>SELECT PULocationID,
            DOLocationID
FROM `dezoomcamp32.yellow_tripdata` </pre>

## Question 4: How many records have fare_amount of 0
<pre>SELECT COUNT(*) AS Zero_fare
FROM `dezoomcamp32.yellow_tripdata`
WHERE fare_amount = 0; </pre>  

## Question 5: Make an optimised table if your query will always filter based on tpep_dropoff_datetime and order the results by VendorID
<pre>CREATE OR REPLACE TABLE `dezoomcamp32.yellow_partitioned_tripdata`
PARTITION BY DATE(tpep_dropoff_datetime ) 
CLUSTER BY VendorID AS (
  SELECT * FROM `dezoomcamp32.yellow_tripdata`
) </pre>

## Question 6: Write a query to retrieve the distinct VendorIDs between tpep_dropoff_datetime 2024-03-01 and 2024-03-15 (inclusive)

### Query from materialised table
<pre>SELECT DISTINCT VendorID
FROM `dezoomcamp32.yellow_tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'; </pre> 

### Query from partitioned table
<pre>SELECT DISTINCT VendorID
FROM `dezoomcamp32.yellow_partitioned_tripdata`
WHERE tpep_dropoff_datetime BETWEEN '2024-03-01' AND '2024-03-15'; </pre>

