SELECT
    s.status,
    s.end_date,
    c.segment,
    c.credit_limit
FROM subscriptions s
INNER JOIN customers c ON s.customer_id = c.id
WHERE c.city != 'New York'
ORDER BY s.status ASC
