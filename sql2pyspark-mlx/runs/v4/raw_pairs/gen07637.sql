SELECT
    customers.city,
    customers.age,
    customers.signup_date,
    sessions.customer_id,
    sessions.country,
    sessions.browser
FROM customers
LEFT JOIN sessions ON sessions.customer_id = customers.id
WHERE customers.signup_date < '2025-07-01' OR customers.city IN ('New York', 'Chiang Mai')
