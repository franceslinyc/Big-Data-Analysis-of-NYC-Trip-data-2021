# Compute monthly avg of total_amount

SELECT 
pickup_mon, 
AVG(total_amount) AS mon_amount, 
FROM trip_data.yellow_data_2019
GROUP BY pickup_mon
ORDER BY pickup_mon ASC;