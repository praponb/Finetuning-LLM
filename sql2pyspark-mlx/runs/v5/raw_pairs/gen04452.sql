SELECT city, SUM(CASE WHEN country = 'DE' THEN rating ELSE 0 END) AS de_rating, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values
FROM suppliers
WHERE country <> 'JP'
GROUP BY city
ORDER BY de_rating DESC;
