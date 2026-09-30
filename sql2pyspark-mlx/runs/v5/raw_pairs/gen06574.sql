SELECT s.id, s.customer_id, c.credit_limit
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE s.country IS NOT NULL;
