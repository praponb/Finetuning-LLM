SELECT
    s.id,
    s.monthly_fee,
    s.end_date,
    s.customer_id,
    c.signup_date,
    c.age,
    c.credit_limit
FROM subscriptions s
INNER JOIN customers c ON c.id = s.customer_id
WHERE c.is_active AND c.name IS NOT NULL
ORDER BY c.signup_date DESC, end_date DESC
