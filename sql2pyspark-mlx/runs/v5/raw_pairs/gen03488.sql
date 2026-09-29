SELECT
    CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END AS rating_bucket,
    SUM(rating) AS total_rating,
    COUNT(*) AS cnt,
    SUM(SUM(rating)) OVER (ORDER BY CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END) AS cumulative_rating
FROM suppliers
GROUP BY CASE WHEN rating >= 3.4 THEN 'high' WHEN rating >= 1.7 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket, cumulative_rating DESC
