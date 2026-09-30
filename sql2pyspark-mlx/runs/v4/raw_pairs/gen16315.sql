SELECT oi.id, oi.product_id, p.category, p.supplier_id, ROUND(oi.unit_price * 0.9, 2) AS unit_price_adjusted FROM order_items oi LEFT JOIN products p ON oi.product_id = p.id ORDER BY p.supplier_id;
