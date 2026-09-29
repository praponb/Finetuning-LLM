SELECT oi.id, oi.quantity, oi.product_id
FROM order_items oi
WHERE oi.unit_price BETWEEN 120 AND 160 AND oi.product_id IN (SELECT id FROM products WHERE stock_qty > 369);
