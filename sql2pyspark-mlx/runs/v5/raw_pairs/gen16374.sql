SELECT orders.id, orders.customer_id FROM orders WHERE orders.customer_id NOT IN (SELECT id FROM customers WHERE city IN ('London', 'Singapore'));
