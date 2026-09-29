SELECT s.region, ROUND(AVG(ss.revenue), 2) AS avg_revenue, MAX(ss.units) AS max_units
FROM store_sales ss
LEFT JOIN stores s ON s.id = ss.store_id
LEFT JOIN products p ON ss.product_id = p.id
WHERE ss.product_id IN (5, 9)
GROUP BY s.region
ORDER BY avg_revenue
