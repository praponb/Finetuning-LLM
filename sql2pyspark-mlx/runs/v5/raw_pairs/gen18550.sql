SELECT *
FROM (
    SELECT CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END AS rating_bucket, CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(country))) AS country_values, SUM(rating) AS total_rating
    FROM suppliers
    WHERE country IN ('TH', 'US')
    GROUP BY CASE WHEN rating >= 4.6 THEN 'high' WHEN rating >= 2.3 THEN 'mid' ELSE 'low' END
) AS sub
WHERE total_rating < 12
ORDER BY rating_bucket;
