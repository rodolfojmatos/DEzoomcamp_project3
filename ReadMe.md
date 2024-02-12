# Week 3 - Homework

##


### SETUP
-- Create an external table using the Green Taxi Trip Records Data for 2022.\
CREATE OR REPLACE EXTERNAL TABLE `dtc-de-378023.nytaxi.external_green_tripdata`
OPTIONS (\
  format = 'PARQUET',\
  uris = ['gs://week3_dezoomcamp/green_tripdata_2022-*.parquet']\
);


-- Create a table in BQ using the Green Taxi Trip Records for 2022 (do not partition or cluster this table).\
CREATE OR REPLACE TABLE dtc-de-378023.nytaxi.green_tripdata_non_partitoned AS\
SELECT * FROM dtc-de-378023.nytaxi.external_green_tripdata;

### Question 1:
--  Question 1: What is count of records for the 2022 Green Taxi Data??\
SELECT COUNT(*) AS trips FROM `dtc-de-378023.nytaxi.external_green_tripdata`;\
or\
SELECT COUNT(*) AS trips FROM `dtc-de-378023.nytaxi.green_tripdata_non_partitoned`;\

- Answer: 840,402



### Question 2:
--Write a query to count the distinct number of PULocationIDs for the entire dataset on both the tables.\
--What is the estimated amount of data that will be read when this query is executed on the External Table and the Table?\

SELECT DISTINCT(PULocationID)\
FROM dtc-de-378023.nytaxi.external_green_tripdata;

SELECT DISTINCT(PULocationID)\
FROM dtc-de-378023.nytaxi.green_tripdata_non_partitoned;

- Answer: 0 MB for the External Table and 6.41MB for the Materialized Table



### Question 3:
-How many records have a fare_amount of 0?

SELECT COUNT(fare_amount) AS Num_fare_amount\
FROM dtc-de-378023.nytaxi.green_tripdata_non_partitoned\
WHERE fare_amount = 0.0;

- Answer: 1622



### Question 4:
-- What is the best strategy to make an optimized table in Big Query if your query will always order the results by PUlocationID and filter based on \
-- lpep_pickup_datetime? (Create a new table with this strategy)

SELECT COUNT(DISTINCT(PULocationID)) AS PU, COUNT(DISTINCT(CAST(lpep_pickup_datetime AS DATE))) AS DATE\
FROM dtc-de-378023.nytaxi.green_tripdata_non_partitoned;

CREATE OR REPLACE TABLE dtc-de-378023.nytaxi.green_tripdata_partitoned_clustered\
PARTITION BY DATE(lpep_pickup_datetime)\
CLUSTER BY PUlocationID AS\
SELECT * FROM dtc-de-378023.nytaxi.external_green_tripdata;

- Answer: Partition by lpep_pickup_datetime Cluster on PUlocationID

### Question 5:
-- Write a query to retrieve the distinct PULocationID between lpep_pickup_datetime 06/01/2022 and 06/30/2022 (inclusive)

SELECT DISTINCT(PULocationID)\
FROM dtc-de-378023.nytaxi.green_tripdata_non_partitoned\
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';\

SELECT DISTINCT(PULocationID)\
FROM dtc-de-378023.nytaxi.green_tripdata_partitoned_clustered\
WHERE DATE(lpep_pickup_datetime) BETWEEN '2022-06-01' AND '2022-06-30';

- Answer: 12.82 MB for non-partitioned table and 1.12 MB for the partitioned table

### Question 6:
- Answer: GCP Bucket

### Question7:
- Answer: True

### Question 8:
SELECT * \
FROM dtc-de-378023.nytaxi.green_tripdata_non_partitoned;

-answer: 114.11MB


