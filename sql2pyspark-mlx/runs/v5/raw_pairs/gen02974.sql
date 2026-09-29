SELECT
    a.id,
    a.balance,
    a.status,
    a.customer_id,
    c.credit_limit
FROM accounts a
JOIN customers c ON c.id = a.customer_id
WHERE c.email LIKE '%@startup.io' AND c.credit_limit > 13600
ORDER BY credit_limit
