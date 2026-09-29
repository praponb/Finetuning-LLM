SELECT
    s.end_date,
    s.customer_id,
    s.status,
    c.age,
    c.name
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE s.status = 'EXPIRED' AND c.credit_limit <= 11100
