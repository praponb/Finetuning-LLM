SELECT
    c.city,
    c.age,
    c.segment,
    s.start_date,
    s.plan,
    s.customer_id
FROM customers c
JOIN subscriptions s ON c.id = s.customer_id
WHERE LOWER(c.name) LIKE '%avi%' AND c.city = 'Singapore'
