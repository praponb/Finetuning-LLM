SELECT o.status, COUNT(oi.id) AS order_item_count FROM orders o INNER JOIN order_items oi ON o.id = oi.order_id GROUP BY o.status ORDER BY status LIMIT 50
