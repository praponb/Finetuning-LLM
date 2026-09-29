SELECT o.id, o.customer_id, o.discount FROM orders o WHERE o.customer_id IN (SELECT id FROM customers WHERE signup_date <= '2023-03-13')
