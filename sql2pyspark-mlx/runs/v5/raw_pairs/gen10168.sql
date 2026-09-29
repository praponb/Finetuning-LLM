SELECT s.id, s.customer_id, s.country
FROM sessions s
WHERE s.customer_id IS NOT NULL AND s.customer_id IN (SELECT id FROM customers WHERE YEAR(signup_date) = 2023)
