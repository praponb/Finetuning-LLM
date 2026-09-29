SELECT
    s.id,
    s.customer_id,
    s.country,
    s.browser
FROM sessions s
WHERE s.customer_id NOT IN (SELECT id FROM customers WHERE age >= 49)
