SELECT payments.method, orders.store_id, orders.customer_id FROM payments RIGHT JOIN orders ON payments.order_id = orders.id WHERE payments.method = 'card' ORDER BY store_id;
