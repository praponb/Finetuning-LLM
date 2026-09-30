SELECT
    CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket,
    city,
    ROUND((100 * SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bangkok,
    COUNT(*) AS total_count
FROM suppliers
GROUP BY CASE WHEN rating >= 4.1 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END, city
HAVING COUNT(*) < 5;
