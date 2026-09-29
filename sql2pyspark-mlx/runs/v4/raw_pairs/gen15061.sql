SELECT o.id, o.customer_id FROM orders o WHERE o.customer_id NOT IN (SELECT id FROM customers WHERE age > 59) ORDER BY customer_id DESC
