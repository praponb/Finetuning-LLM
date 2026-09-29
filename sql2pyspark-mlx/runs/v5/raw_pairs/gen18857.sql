SELECT
    CASE WHEN rating >= 4.4 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket,
    country,
    ROUND(AVG(rating), 2) AS avg_rating,
    COUNT(CASE WHEN country = 'UK' THEN 1 END) AS uk_count
FROM suppliers
WHERE country IN ('US', 'TH') AND rating <= 3.3
GROUP BY rating_bucket, country
ORDER BY uk_count, rating_bucket, country
LIMIT 1;
