SELECT
    c.segment,
    c.signup_date,
    s.plan,
    s.end_date
FROM customers c
LEFT OUTER JOIN subscriptions s ON s.customer_id = c.id
WHERE c.city IN ('London', 'Chiang Mai')
ORDER BY segment;
