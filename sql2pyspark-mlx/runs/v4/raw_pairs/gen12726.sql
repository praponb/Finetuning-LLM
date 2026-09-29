SELECT s.city, COUNT(DISTINCT ss.units) AS unique_stores, MAX(ss.units) - MIN(ss.units) AS units_range
FROM store_sales ss
INNER JOIN products p ON ss.product_id = p.id
INNER JOIN stores s ON ss.store_id = s.id
GROUP BY s.city
