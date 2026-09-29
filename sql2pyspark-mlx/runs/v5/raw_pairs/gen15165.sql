SELECT CASE WHEN s.rating >= 4.2 THEN 'high' WHEN s.rating >= 2.0 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(oi.quantity) AS quantity_count
FROM order_items oi
JOIN products p ON oi.product_id = p.id
JOIN suppliers s ON s.id = p.supplier_id
WHERE oi.product_id IN (4, 23, 29)
GROUP BY CASE WHEN s.rating >= 4.2 THEN 'high' WHEN s.rating >= 2.0 THEN 'mid' ELSE 'low' END
