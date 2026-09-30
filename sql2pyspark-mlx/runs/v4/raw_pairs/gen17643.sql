SELECT
    s.id,
    s.country,
    s.customer_id,
    s.started_at
FROM sessions s
WHERE s.browser IN ('Edge', 'Safari') AND s.customer_id NOT IN (SELECT id FROM customers WHERE segment = 'Consumer')
ORDER BY customer_id
