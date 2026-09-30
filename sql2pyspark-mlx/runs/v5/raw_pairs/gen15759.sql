SELECT city, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values, AVG(rating) AS avg_rating, SORT_ARRAY(COLLECT_SET(city)) AS city_list FROM suppliers GROUP BY city ORDER BY city ASC;
