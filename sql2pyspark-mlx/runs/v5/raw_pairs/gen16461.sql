SELECT oi.id, oi.quantity, oi.unit_price, oi.product_id FROM order_items oi WHERE oi.order_id NOT IN (SELECT id FROM orders WHERE shipping_fee < 36);
