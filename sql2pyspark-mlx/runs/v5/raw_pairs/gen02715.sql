SELECT
    c.id,
    c.signup_date,
    c.credit_limit,
    c.country
FROM customers c
WHERE EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.end_date > '2025-03-01')
