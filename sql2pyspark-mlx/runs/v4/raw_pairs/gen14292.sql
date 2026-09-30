SELECT
    t.id,
    t.amount,
    t.account_id,
    a.customer_id,
    a.status,
    c.city,
    c.country
FROM transactions t
JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
WHERE c.email IS NULL;
