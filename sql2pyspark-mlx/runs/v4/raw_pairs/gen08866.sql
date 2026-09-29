SELECT o.store_id, oi.product_id, oi.order_id FROM orders o LEFT JOIN order_items oi ON oi.order_id = o.id WHERE o.store_id = 32 AND oi.product_id IN (4, 15, 21) ORDER BY o.store_id DESC;
