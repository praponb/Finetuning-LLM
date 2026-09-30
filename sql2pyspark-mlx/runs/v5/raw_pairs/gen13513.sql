SELECT
    s.id,
    s.device,
    s.customer_id,
    s.country,
    c.is_active
FROM sessions s
INNER JOIN customers c ON s.customer_id = c.id
