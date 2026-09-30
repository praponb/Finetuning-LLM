SELECT
    s.browser,
    s.device,
    s.started_at,
    c.country
FROM sessions s
JOIN customers c ON s.customer_id = c.id;
