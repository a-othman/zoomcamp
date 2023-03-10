-- q1:
SELECT count(*) FROM `zoomcamp-375102.rides.2019`

-- q2:
SELECT  COUNT(DISTINCT Affiliated_base_number) FROM `zoomcamp-375102.bq.bq_rides`


-- q3:
SELECT  COUNT(*) 
FROM `zoomcamp-375102.bq.bq_rides` r
where r.PUlocationID is null and r.DOlocationID is null


-- q5
CREATE OR REPLACE TABLE `zoomcamp-375102.bq.bq_partitioned`
PARTITION BY
  DATE(pickup_datetime) AS
SELECT * FROM `zoomcamp-375102.bq.bq_rides`;

select distinct r.Affiliated_base_number
from `zoomcamp-375102.bq.bq_partitioned` r
where DATE(r.pickup_datetime) >= '2019-03-01' and DATE(r.pickup_datetime) <= '2019-03-31'