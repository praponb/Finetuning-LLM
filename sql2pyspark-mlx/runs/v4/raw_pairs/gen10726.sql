SELECT oi.id, oi.order_id, oi.product_id FROM order_items oi WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE channel = 'partner');
