SELECT
    t.merchant,
    t.account_id,
    a.balance,
    c.is_active,
    c.age
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
INNER JOIN customers c ON a.customer_id = c.id
WHERE a.opened_date > '2025-03-17' AND t.amount > 3900
ORDER BY t.account_id DESC, age DESC
