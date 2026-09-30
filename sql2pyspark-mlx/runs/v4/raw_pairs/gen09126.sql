SELECT
    c.id,
    c.city,
    c.credit_limit,
    c.signup_date
FROM customers c
WHERE c.id IN (SELECT customer_id FROM subscriptions WHERE customer_id IS NOT NULL);
