SELECT o.id, COUNT(oi.id) AS order_item_count FROM orders o LEFT OUTER JOIN order_items oi ON o.id = oi.order_id WHERE oi.unit_price > 210 GROUP BY o.id
