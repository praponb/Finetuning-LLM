SELECT country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values FROM customers WHERE city IN ('Tokyo', 'Berlin', 'New York') GROUP BY country
