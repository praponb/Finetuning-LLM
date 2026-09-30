SELECT s.id, s.started_at
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE email LIKE '%@gmail.com');
