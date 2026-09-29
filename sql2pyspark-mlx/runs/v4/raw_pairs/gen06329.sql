SELECT country, ROUND((100 * SUM(CASE WHEN city = 'Bangkok' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_bangkok
FROM customers
WHERE name LIKE '%Chen%'
GROUP BY country
