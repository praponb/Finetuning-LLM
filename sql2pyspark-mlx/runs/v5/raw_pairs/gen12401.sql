SELECT s.city, AVG(ss.units) AS avg_units, MAX(ss.revenue) - MIN(ss.revenue) AS revenue_range
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
GROUP BY s.city
HAVING revenue_range >= 70
