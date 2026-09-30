SELECT * FROM (SELECT city, MAX(rating) AS max_rating, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values FROM suppliers GROUP BY city) AS s WHERE max_rating < 2.8
