SELECT region, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values, SUM(sqft) AS total_sqft
FROM stores
GROUP BY region
ORDER BY region
