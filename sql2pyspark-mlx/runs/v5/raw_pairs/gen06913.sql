SELECT o.id, o.shipping_fee, o.channel FROM orders o WHERE EXISTS (SELECT 1 FROM order_items oi WHERE oi.order_id = o.id AND oi.product_id IN (3, 13, 15, 20)) ORDER BY o.channel ASC, id ASC LIMIT 1;
