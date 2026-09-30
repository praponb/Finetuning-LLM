SELECT city, ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london
FROM stores
WHERE region IS NOT NULL AND LOWER(store_name) LIKE '%own%' AND opened_date >= '2025-07-01'
GROUP BY city
HAVING ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) < 74
ORDER BY pct_london DESC, city DESC;
