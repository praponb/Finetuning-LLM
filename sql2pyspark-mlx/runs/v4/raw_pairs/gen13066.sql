SELECT ROUND(STDDEV(rating), 2) AS stddev_rating, AVG(rating) AS avg_rating, SORT_ARRAY(COLLECT_SET(country)) AS country_list FROM suppliers ORDER BY stddev_rating DESC;
