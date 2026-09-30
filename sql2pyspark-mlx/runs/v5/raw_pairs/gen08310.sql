SELECT
    s.id,
    s.plan,
    s.end_date,
    c.age
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE s.customer_id IN (24, 36)
