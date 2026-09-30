SELECT orders.id, orders.status, COUNT(order_items.id) AS order_item_count FROM orders LEFT JOIN order_items ON orders.id = order_items.order_id GROUP BY orders.id, orders.status
