SELECT s.country, AVG(ss.revenue) AS avg_revenue
FROM store_sales ss
JOIN products p ON ss.product_id = p.id
JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country;
