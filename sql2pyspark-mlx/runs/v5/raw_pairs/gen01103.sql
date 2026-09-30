SELECT s.id, s.country
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE signup_date > '2023-07-01');
