SELECT
    c.email,
    c.age,
    s.monthly_fee,
    s.plan,
    s.customer_id,
    UPPER(s.status) AS status_upper
FROM customers c
JOIN subscriptions s ON s.customer_id = c.id
ORDER BY status_upper;
