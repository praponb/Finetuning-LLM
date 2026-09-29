SELECT CASE WHEN s.rating >= 4.4 THEN 'high' WHEN s.rating >= 1.8 THEN 'mid' ELSE 'low' END AS rating_bucket, MAX(i.last_restock) AS last_last_restock, COUNT(DISTINCT i.quantity) AS unique_stores
FROM inventory i
LEFT JOIN products p ON i.product_id = p.id
RIGHT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 4.4 THEN 'high' WHEN s.rating >= 1.8 THEN 'mid' ELSE 'low' END
