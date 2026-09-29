SELECT
    c.email,
    s.started_at,
    s.device,
    s.customer_id
FROM customers c
LEFT JOIN sessions s ON c.id = s.customer_id;
