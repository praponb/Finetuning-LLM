SELECT oi.id, oi.quantity, o.customer_id FROM order_items oi INNER JOIN orders o ON oi.order_id = o.id;
