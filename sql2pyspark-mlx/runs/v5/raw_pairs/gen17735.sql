SELECT p.brand, AVG(ss.revenue) AS avg_revenue
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN stores s ON ss.store_id = s.id
WHERE ss.product_id IN (5, 8)
GROUP BY p.brand
