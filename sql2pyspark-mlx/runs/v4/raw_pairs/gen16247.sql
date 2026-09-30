SELECT
    s.start_date,
    s.plan,
    c.signup_date,
    LOWER(c.segment) AS segment_lower
FROM subscriptions s
LEFT JOIN customers c ON c.id = s.customer_id
WHERE c.email LIKE '%@outlook.com'
ORDER BY s.plan DESC
