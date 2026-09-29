SELECT city, SORT_ARRAY(COLLECT_SET(region)) AS region_list, MIN(sqft) AS min_sqft
FROM stores
GROUP BY city
HAVING MIN(sqft) > 6300
ORDER BY min_sqft DESC
