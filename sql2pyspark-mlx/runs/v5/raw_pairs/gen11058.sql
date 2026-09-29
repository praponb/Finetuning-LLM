SELECT
    country,
    MAX(rating) AS max_rating,
    AVG(rating) AS avg_rating,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
FROM suppliers
WHERE city = 'London'
GROUP BY country
ORDER BY country;
