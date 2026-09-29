SELECT o.id, o.order_date FROM orders o WHERE o.id IN (SELECT order_id FROM payments);
