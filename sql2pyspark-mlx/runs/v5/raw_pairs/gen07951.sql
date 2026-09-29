SELECT s.id, s.browser, s.started_at
FROM sessions s
WHERE s.customer_id IS NULL AND s.customer_id NOT IN (SELECT id FROM customers WHERE signup_date < '2024-02-27')
