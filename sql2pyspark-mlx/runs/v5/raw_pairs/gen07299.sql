SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(region))) AS region_values, MAX(sqft) AS max_sqft FROM stores GROUP BY city HAVING MAX(sqft) < 11200 ORDER BY region_values DESC
