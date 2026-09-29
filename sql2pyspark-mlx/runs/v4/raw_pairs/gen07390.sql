SELECT s.city, AVG(ss.units) AS avg_units, COUNT(DISTINCT ss.units) AS unique_stores
FROM store_sales ss
INNER JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
WHERE ss.units BETWEEN 19 AND 28
GROUP BY s.city
ORDER BY s.city
