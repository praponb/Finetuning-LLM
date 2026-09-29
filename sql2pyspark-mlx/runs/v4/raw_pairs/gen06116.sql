SELECT
    s.id,
    s.customer_id,
    s.browser,
    s.country
FROM sessions s
WHERE s.customer_id IN (SELECT id FROM customers WHERE segment IN ('SMB', 'Consumer'))
ORDER BY s.browser ASC;
