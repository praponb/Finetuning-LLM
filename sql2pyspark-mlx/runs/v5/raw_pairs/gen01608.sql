SELECT city, SUM(CASE WHEN city = 'Tokyo' THEN rating ELSE 0 END) AS tokyo_rating, ROUND((100 * SUM(CASE WHEN country = 'DE' THEN 1 ELSE 0 END)) / COUNT(*), 1) AS pct_de
FROM suppliers
WHERE supplier_name IS NOT NULL
GROUP BY 1
ORDER BY pct_de;
