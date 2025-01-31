# Module 2 Homework: Workflow Orchestration with Kestra

## Question 3
### sql script to calculate the total number of yellow taxi data rows for all csv files in year 2020 in bigquery
<pre>   SELECT COUNT(*) AS row_count
        FROM `dtc-de-course-448012.dezoomcamp32.yellow_tripdata`
        WHERE filename LIKE 'yellow_tripdata_2020-%.csv'; </pre>

## Question 4
### sql script to calculate the total number of green taxi data rows for all csv files in year 2020 in bigquery
<pre>   SELECT COUNT(*) AS row_count
        FROM `dtc-de-course-448012.dezoomcamp32.green_tripdata`
        WHERE filename LIKE 'green_tripdata_2020-%.csv'; </pre>

## Question 5
### sql script to calculate the total number of yellow taxi data rows for all csv files in year 2021 and month **March** in bigquery
<pre>   SELECT COUNT(*) AS row_count
        FROM `dtc-de-course-448012.dezoomcamp32.yellow_tripdata`
        WHERE filename LIKE 'yellow_tripdata_2021-03%.csv'; </pre>