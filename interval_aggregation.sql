CREATE TABLE IF NOT EXISTS stationstatus (
	ID integer, 
	STATION_ID integer, 
	TIMESTAMP timestamp, 
	NUM_BIKES integer,
	NUM_DOCKS integer,
	CAPACITY integer);

--*****filter and create table with only the 2012 season:
SELECT * INTO stationstatus2012 
FROM stationstatus 
WHERE timestamp >= '2012-05-01'
and timestamp < '2012-10-01'
ORDER BY timestamp

--*****Calculate Arrival Rate and Depature Rate for each interval and the number of bikes and capacity at the end of each interval 
WITH t1 as (SELECT *, to_timestamp(extract(epoch from timestamp)::integer/300*300) AT TIME ZONE '-00:00' AS interv, 
		   num_bikes - lag(num_bikes) OVER(partition by station_id ORDER BY timestamp) as bike_arrival,
		   capacity - lag(capacity) OVER(partition by station_id ORDER BY timestamp) as delta_capacity 
		   --lag(capacity) OVER(partition by station_id ORDER BY timestamp) as lag_capacity,
		   --num_docks - lag(num_docks) OVER(partition by station_id ORDER BY timestamp) as dock_liberation,
	    FROM  stationstatus2012), 
     t2 as (SELECT station_id, timestamp,num_bikes, num_docks, capacity, interv,
		   CASE WHEN bike_arrival <= 0 THEN -bike_arrival ELSE 0 END AS bike_departures,
		   CASE WHEN bike_arrival >= 0 THEN bike_arrival ELSE 0 END AS bike_arrival,
		   delta_capacity,
		   first_value(num_bikes) OVER(partition by interv, station_id ORDER BY timestamp desc) as latest_num_bikes,
		   first_value(capacity) OVER(partition by interv, station_id ORDER BY timestamp desc) as latest_capacity
	    FROM t1),
     t3 as (SELECT station_id, interv, sum(bike_arrival) as bike_arrivals, 
                   sum(bike_departures) as bike_departures, 
                   min(latest_num_bikes) as latest_num_bikes, 
                   min(latest_capacity) as latest_capacity
            FROM t2
            GROUP BY station_id, interv
            ORDER BY station_id, interv),
     dates as (SELECT generate_series('2012-05-01 00:00:00'::timestamp, '2012-10-01 00:00:00'::timestamp, interval '5 minutes') as interv),
     stations as (SELECT distinct station_id FROM stationstatus2012),
     full_interv as (SELECT* FROM dates JOIN stations ON TRUE ORDER BY interv, station_id)

SELECT b.*, a.bike_arrivals, a.bike_departures, a.latest_num_bikes, a.latest_capacity
INTO status_interval_2012_2 
FROM t3 as a RIGHT JOIN full_interv as b ON a.interv = b.interv AND a.station_id=b.station_id
ORDER BY interv, station_id


-- EXPORT RESULTS
SELECT * 
FROM status_interval_2012_2
ORDER BY interv, station_id
