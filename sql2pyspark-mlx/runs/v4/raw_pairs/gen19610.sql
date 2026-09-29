SELECT
    city,
    CASE WHEN rating >= 3.6 THEN 'high' WHEN rating >= 1.4 THEN 'mid' ELSE 'low' END AS rating_bucket,
    MAX(rating) - MIN(rating) AS rating_range,
    SUM(rating) AS total_rating
FROM suppliers
WHERE country = 'UK' AND country IN ('JP', 'US', 'UK')
GROUP BY 1, 2
ORDER BY total_rating DESC;
