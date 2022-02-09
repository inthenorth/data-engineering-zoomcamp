--Question 1
SELECT count(*) FROM `crested-acumen-339310.nytaxi.fhv_data` 
--Question 2
SELECT count(distinct (dispatching_base_num)) FROM `crested-acumen-339310.nytaxi.fhv_data`
--Question 3
CREATE OR REPLACE TABLE nytaxi.fhv_part
PARTITION BY
  DATE(dropoff_datetime) AS
SELECT * FROM nytaxi.fhv_data
--Question 4
SELECT count(*) FROM `crested-acumen-339310.nytaxi.fhv_data`WHERE dispatching_base_num in('B00987','B02060','B02279')  
AND dropoff_datetime BETWEEN '2019-01-01'AND '2019-03-31'

