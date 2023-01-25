-- question 3:
select count(*)
from green_taxi
where date_trunc('day', lpep_pickup_datetime)='2019-01-15' and 
		date_trunc('day', lpep_dropoff_datetime)='2019-01-15'
		
-- question 4:
select *
from green_taxi
where trip_distance= (select max(trip_distance) from green_taxi)

-- question 5:
select passenger_count, count(passenger_count)
from green_taxi
where passenger_count in (2,3) and 
	date_trunc('day', lpep_pickup_datetime)='2019-01-01'
group by 1

-- question 6:
part 1
select *
from (
select *
from green_taxi
where green_taxi."PULocationID" in (select zones."LocationID"
					from zones
					where zones."Zone"='Astoria')
	and tip_amount=88
part. 2
select *
from zones
where zones."LocationID"=146
