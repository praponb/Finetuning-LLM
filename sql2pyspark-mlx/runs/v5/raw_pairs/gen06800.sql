SELECT
    s.id,
    s.device,
    s.browser,
    s.customer_id,
    c.age,
    c.email
FROM sessions s
FULL OUTER JOIN customers c ON c.id = s.customer_id
WHERE NOT (s.customer_id IS NOT NULL AND s.customer_id IN (4, 28))
