SELECT oi.id, oi.quantity, o.amount, o.customer_id FROM order_items oi JOIN orders o ON o.id = oi.order_id WHERE o.amount < 1040
