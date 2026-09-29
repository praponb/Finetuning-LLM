SELECT segment, SORT_ARRAY(COLLECT_SET(city)) AS city_list FROM customers WHERE age = 60 GROUP BY segment ORDER BY segment;
