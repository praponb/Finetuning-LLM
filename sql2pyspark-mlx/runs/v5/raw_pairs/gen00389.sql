SELECT
    city,
    CASE WHEN rating >= 3.6 THEN 'LARGE' ELSE 'SMALL' END AS rating_tier,
    SUM(CASE WHEN country = 'UK' THEN rating ELSE 0 END) AS uk_rating,
    ROUND(AVG(rating), 2) AS avg_rating
FROM suppliers
GROUP BY GROUPING SETS ((city, CASE WHEN rating >= 3.6 THEN 'LARGE' ELSE 'SMALL' END), (city))
ORDER BY rating_tier ASC NULLS LAST;
