SELECT city, SORT_ARRAY(COLLECT_SET(city)) AS city_list, MAX(sqft) - MIN(sqft) AS sqft_range FROM stores GROUP BY city ORDER BY sqft_range ASC
