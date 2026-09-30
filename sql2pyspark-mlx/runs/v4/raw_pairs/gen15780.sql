SELECT
    s.id,
    s.monthly_fee,
    c.signup_date,
    c.credit_limit,
    c.email
FROM subscriptions s
FULL OUTER JOIN customers c ON s.customer_id = c.id
WHERE s.plan = 'basic'
ORDER BY email ASC
