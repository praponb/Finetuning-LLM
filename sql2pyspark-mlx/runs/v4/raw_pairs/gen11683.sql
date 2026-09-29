SELECT c.signup_date, c.email, o.customer_id, o.amount, o.order_date FROM customers c LEFT JOIN orders o ON o.customer_id = c.id WHERE c.signup_date <= '2023-09-01'
