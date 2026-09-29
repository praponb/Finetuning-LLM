SELECT s.id, s.country
FROM sessions s
WHERE s.customer_id IN (5, 18, 38) AND s.customer_id IN (SELECT id FROM customers WHERE credit_limit IS NOT NULL)
ORDER BY country, s.id DESC
