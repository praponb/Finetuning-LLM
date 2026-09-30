SELECT country, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(segment))) AS segment_values FROM customers GROUP BY 1 ORDER BY country LIMIT 50
