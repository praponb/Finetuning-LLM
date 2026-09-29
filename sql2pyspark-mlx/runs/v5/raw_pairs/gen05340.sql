SELECT
    s.id,
    s.customer_id,
    s.device,
    s.started_at,
    c.email,
    c.city,
    c.age
FROM sessions s
JOIN customers c ON s.customer_id = c.id
WHERE s.started_at >= '2024-01-01';
