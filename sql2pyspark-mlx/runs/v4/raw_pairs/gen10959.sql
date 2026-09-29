SELECT
    CASE WHEN rating >= 4.3 THEN 'PREMIUM' ELSE 'STANDARD' END AS rating_tier,
    city,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN city = 'Singapore' THEN 1 END), 0), 2) AS rating_per_singapore,
    ROUND(SUM(rating) / NULLIF(COUNT(CASE WHEN country = 'SG' THEN 1 END), 0), 2) AS rating_per_sg,
    COUNT(CASE WHEN country = 'US' THEN 1 END) AS us_count,
    ROUND(AVG(rating), 2) AS avg_rating,
    COUNT(CASE WHEN city = 'Singapore' THEN 1 END) AS singapore_count
FROM suppliers
GROUP BY GROUPING SETS ((CASE WHEN rating >= 4.3 THEN 'PREMIUM' ELSE 'STANDARD' END, city), (CASE WHEN rating >= 4.3 THEN 'PREMIUM' ELSE 'STANDARD' END), ())
ORDER BY rating_per_sg DESC;
