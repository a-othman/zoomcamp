-- q1:
SELECT count(*) FROM `zoomcamp-375102.rides.2019`

-- q2:
SELECT  COUNT(DISTINCT Affiliated_base_number) FROM `zoomcamp-375102.bq.bq_rides`


-- q3:
SELECT  COUNT(*) 
FROM `zoomcamp-375102.bq.bq_rides` r
where r.PUlocationID is null and r.DOlocationID is null