SELECT oi.id, oi.product_id, oi.unit_price, oi.quantity FROM order_items oi WHERE oi.order_id IN (SELECT id FROM orders WHERE order_date > '2024-06-01') ORDER BY product_id DESC, oi.quantity DESC;
