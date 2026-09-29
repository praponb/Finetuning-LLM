SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.country
FROM sessions s
WHERE s.customer_id IN (6, 12, 14) AND s.customer_id IN (SELECT id FROM customers WHERE age <= 64)
ORDER BY s.browser DESC, id
LIMIT 3;
