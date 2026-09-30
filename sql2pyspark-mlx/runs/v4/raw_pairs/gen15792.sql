SELECT region, city, SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM stores
GROUP BY region, city;
