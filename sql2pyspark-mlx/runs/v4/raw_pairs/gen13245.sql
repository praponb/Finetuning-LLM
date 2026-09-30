SELECT
    c.id,
    c.credit_limit,
    c.city,
    c.signup_date
FROM customers c
WHERE c.age BETWEEN 34 AND 63 AND NOT EXISTS (SELECT 1 FROM subscriptions s WHERE s.customer_id = c.id AND s.monthly_fee <= 80)
ORDER BY id;
