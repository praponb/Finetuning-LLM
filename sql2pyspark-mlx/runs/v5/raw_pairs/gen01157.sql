SELECT
    customers.id,
    customers.signup_date,
    customers.email,
    customers.age
FROM customers
WHERE NOT EXISTS (SELECT 1 FROM sessions WHERE sessions.customer_id = customers.id AND sessions.started_at < '2024-03-01');
