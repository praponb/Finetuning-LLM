SELECT
    CASE WHEN rating >= 3.8 THEN 'high' WHEN rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket,
    country,
    MIN(rating) AS min_rating,
    COUNT(DISTINCT rating) AS unique_countrys
FROM suppliers
WHERE rating > 2.7 AND rating <= 2.8 AND city NOT IN ('London', 'Tokyo', 'Bangkok')
GROUP BY rating_bucket, country;
