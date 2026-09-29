SELECT
    CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(rating) AS total_rating,
    COUNT(*) AS row_count,
    SORT_ARRAY(COLLECT_SET(city)) AS city_list
FROM suppliers
GROUP BY rating_bucket
ORDER BY row_count DESC;
