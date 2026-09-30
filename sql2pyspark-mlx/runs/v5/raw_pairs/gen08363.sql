SELECT
    s.id,
    s.browser,
    s.device,
    s.country
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE segment IS NULL);
