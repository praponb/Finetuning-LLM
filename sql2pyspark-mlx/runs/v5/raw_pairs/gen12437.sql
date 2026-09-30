SELECT location, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(location))) AS location_values, COUNT(*) AS total_count
FROM departments
GROUP BY location
HAVING COUNT(*) >= 1;
