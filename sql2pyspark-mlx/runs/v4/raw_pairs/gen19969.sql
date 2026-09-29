SELECT
    a.id,
    a.account_type,
    a.customer_id,
    c.is_active,
    c.age
FROM accounts a
INNER JOIN customers c ON c.id = a.customer_id
WHERE c.age <> 57
ORDER BY age DESC;
