SELECT
    s.id,
    s.browser,
    s.country,
    s.device
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE credit_limit <= 11600)
