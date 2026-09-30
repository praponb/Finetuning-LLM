SELECT CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(DISTINCT rating) AS unique_countrys, ROUND((100 * SUM(CASE WHEN city = 'Berlin' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_berlin
FROM suppliers
GROUP BY CASE WHEN rating >= 4.3 THEN 'high' WHEN rating >= 1.6 THEN 'mid' ELSE 'low' END
ORDER BY unique_countrys DESC;
