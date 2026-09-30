SELECT oi.id, oi.unit_price FROM order_items oi WHERE oi.order_id IN (SELECT id FROM orders WHERE store_id IN (6, 16, 23, 34))
