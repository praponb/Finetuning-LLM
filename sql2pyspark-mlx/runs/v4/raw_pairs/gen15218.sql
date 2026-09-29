SELECT
    s.id,
    s.device,
    s.country,
    s.started_at
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE country = 'TH')
ORDER BY s.started_at, id
LIMIT 10;
