SELECT s.id, s.country FROM sessions s WHERE TO_DATE(s.started_at) < '2023-12-01' AND s.customer_id NOT IN (SELECT id FROM customers WHERE age >= 31);
