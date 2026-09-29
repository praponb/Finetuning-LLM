SELECT
    s.id,
    s.browser,
    s.started_at,
    c.credit_limit,
    c.city
FROM sessions s
LEFT JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id IN (13, 18, 26, 27) OR s.country = 'JP'
