SELECT CASE WHEN s.rating >= 4.2 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END AS rating_bucket, COUNT(oi.quantity) AS quantity_count, MAX(oi.unit_price) - MIN(oi.unit_price) AS unit_price_range
FROM order_items oi
INNER JOIN products p ON p.id = oi.product_id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY CASE WHEN s.rating >= 4.2 THEN 'high' WHEN s.rating >= 2.4 THEN 'mid' ELSE 'low' END
ORDER BY rating_bucket DESC;
