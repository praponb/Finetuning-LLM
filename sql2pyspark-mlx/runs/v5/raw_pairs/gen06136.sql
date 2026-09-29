SELECT
    a.id,
    a.opened_date,
    c.city,
    c.age
FROM accounts a
LEFT JOIN customers c ON c.id = a.customer_id
ORDER BY age DESC;
