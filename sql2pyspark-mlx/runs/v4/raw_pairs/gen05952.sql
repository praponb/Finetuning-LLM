SELECT
    s.id,
    s.started_at,
    s.customer_id,
    s.country,
    c.is_active,
    c.email
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE LENGTH(c.name) < 9 AND c.age <> 67;
