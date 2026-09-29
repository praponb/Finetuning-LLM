SELECT
    country,
    CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(CASE WHEN city = 'Singapore' THEN rating ELSE 0 END) AS singapore_rating,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values
FROM suppliers
WHERE LENGTH(supplier_name) >= 9 AND rating >= 3.0
GROUP BY 1, 2
ORDER BY rating_bucket;
