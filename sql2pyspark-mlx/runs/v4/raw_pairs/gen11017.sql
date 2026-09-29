SELECT o.store_id, oi.quantity, oi.product_id FROM orders o INNER JOIN order_items oi ON oi.order_id = o.id WHERE NOT (o.store_id = 29 OR o.amount BETWEEN 930 AND 1040) ORDER BY store_id
