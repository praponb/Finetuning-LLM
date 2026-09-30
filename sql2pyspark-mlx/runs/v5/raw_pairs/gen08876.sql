SELECT s.id, s.end_date FROM subscriptions s WHERE s.customer_id IN (SELECT id FROM customers WHERE signup_date BETWEEN '2023-11-03' AND '2024-05-01') ORDER BY id LIMIT 10
