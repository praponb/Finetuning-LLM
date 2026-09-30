SELECT o.id, o.amount, o.customer_id, o.channel FROM orders o WHERE NOT EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.product_id IN (28, 32, 36));
