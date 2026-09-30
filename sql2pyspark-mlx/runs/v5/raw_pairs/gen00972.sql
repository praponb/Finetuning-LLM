SELECT
    s.id,
    s.monthly_fee,
    c.email,
    c.credit_limit
FROM subscriptions s
LEFT OUTER JOIN customers c ON s.customer_id = c.id
WHERE c.credit_limit > 11500 AND YEAR(s.start_date) = 2024
ORDER BY s.id DESC
