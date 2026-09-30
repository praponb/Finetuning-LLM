SELECT s.country, COUNT(*) AS num_order_items, SUM(oi.unit_price) AS total_unit_price
FROM order_items oi
LEFT JOIN products p ON oi.product_id = p.id
LEFT JOIN suppliers s ON p.supplier_id = s.id
GROUP BY s.country
