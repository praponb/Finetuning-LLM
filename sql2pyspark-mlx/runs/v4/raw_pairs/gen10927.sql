SELECT oi.id, oi.unit_price, oi.order_id, oi.quantity FROM order_items oi WHERE oi.order_id IN (SELECT id FROM orders WHERE channel = 'store');
