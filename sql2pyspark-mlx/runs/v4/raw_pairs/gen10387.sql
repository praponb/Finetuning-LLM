SELECT *
FROM (
    SELECT
        CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END AS rating_bucket,
        country,
        ROUND(AVG(rating), 2) AS avg_rating,
        MAX(rating) - MIN(rating) AS rating_range
    FROM suppliers
    GROUP BY CASE WHEN rating >= 3.7 THEN 'high' WHEN rating >= 1.9 THEN 'mid' ELSE 'low' END, country
) AS x
WHERE avg_rating > 33
ORDER BY rating_range, rating_bucket;
