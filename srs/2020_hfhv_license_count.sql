# Compute count yearly license

SELECT 
license, 
COUNT(license) AS count_license
FROM trip_data.hfhv_data_2020
GROUP BY license
ORDER BY count_license DESC;