SELECT country, ROUND(STDDEV(rating), 2) AS stddev_rating, COUNT(rating) AS rating_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values FROM suppliers GROUP BY country
