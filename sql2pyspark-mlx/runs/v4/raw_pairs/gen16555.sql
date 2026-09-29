SELECT country, city, COUNT(rating) AS rating_count, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values FROM suppliers GROUP BY ROLLUP(country, city) ORDER BY 4 DESC
