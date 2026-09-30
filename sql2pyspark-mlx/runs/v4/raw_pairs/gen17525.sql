SELECT s.region, COUNT(DISTINCT ss.units) AS unique_products, ROUND(AVG(ss.units), 2) AS avg_units
FROM store_sales ss
LEFT JOIN stores s ON ss.store_id = s.id
INNER JOIN products p ON ss.product_id = p.id
GROUP BY s.region
HAVING COUNT(DISTINCT ss.units) >= 5
