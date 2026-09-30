SELECT oi.id, oi.order_id, o.customer_id, p.price FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id INNER JOIN products p ON oi.product_id = p.id WHERE o.channel = 'web' ORDER BY id
