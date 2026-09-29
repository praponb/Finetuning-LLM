SELECT
    city,
    CASE WHEN rating >= 3.5 THEN 'high' WHEN rating >= 2.1 THEN 'mid' ELSE 'low' END rating_bucket,
    country,
    SUM(CASE WHEN country = 'JP' THEN rating ELSE 0 END) jp_rating,
    MAX(rating) - MIN(rating) rating_range,
    SUM(rating) total_rating
FROM suppliers
WHERE LOWER(supplier_name) LIKE '%iam%' AND (supplier_name IS NOT NULL OR country = 'US')
GROUP BY 1, 2, 3
HAVING MAX(rating) - MIN(rating) >= 69
ORDER BY country, 5
