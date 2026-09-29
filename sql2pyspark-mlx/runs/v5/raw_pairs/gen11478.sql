SELECT
    s.id,
    s.customer_id,
    s.browser,
    c.name,
    c.is_active
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id;
