SELECT oi.id, oi.quantity, oi.order_id, oi.product_id FROM order_items oi WHERE oi.order_id IN (SELECT id FROM orders WHERE shipping_fee < 28) ORDER BY oi.quantity DESC;
