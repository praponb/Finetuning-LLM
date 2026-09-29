SELECT
    c.age,
    c.credit_limit,
    s.monthly_fee,
    s.start_date,
    s.customer_id
FROM customers c
LEFT OUTER JOIN subscriptions s ON s.customer_id = c.id
ORDER BY age DESC, start_date DESC;
