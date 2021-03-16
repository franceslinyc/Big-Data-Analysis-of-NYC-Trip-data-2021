# Compute top 6 count of DOLocationID 

SELECT
DISTINCT DOLocationID, 
COUNT(DOLocationID) as DOLocationID_count
FROM trip_data.hfhv_data_2020
GROUP BY DOLocationID
ORDER BY DOLocationID_count DESC
LIMIT 6; 