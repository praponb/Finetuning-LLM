SELECT customers.city, orders.status, orders.store_id, orders.amount FROM customers LEFT OUTER JOIN orders ON customers.id = orders.customer_id
