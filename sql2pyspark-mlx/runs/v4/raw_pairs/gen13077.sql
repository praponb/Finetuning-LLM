SELECT
    s.id,
    s.country,
    s.started_at,
    s.device
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE segment IS NOT NULL);
