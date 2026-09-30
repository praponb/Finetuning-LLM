SELECT oi.id, oi.quantity, o.order_date, o.amount, s.store_name FROM order_items oi RIGHT JOIN orders o ON oi.order_id = o.id JOIN stores s ON o.store_id = s.id
