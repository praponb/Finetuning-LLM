SELECT
    CASE WHEN rating >= 4.0 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(city))) AS city_values,
    MIN(rating) AS min_rating,
    ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
GROUP BY rating_bucket;
