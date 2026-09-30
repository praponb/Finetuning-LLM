SELECT MIN(rating) AS min_rating, ROUND((100 * SUM(CASE WHEN city = 'London' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_london
FROM suppliers
