SELECT s.region, ROUND(AVG(ss.units), 2) AS avg_units
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
WHERE YEAR(ss.sale_date) >= 2025
GROUP BY s.region
HAVING ROUND(AVG(ss.units), 2) >= 73
ORDER BY avg_units DESC;
