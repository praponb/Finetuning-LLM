SELECT
    t.id,
    t.txn_time,
    t.txn_type,
    a.balance,
    a.status,
    c.signup_date
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
LEFT JOIN customers c ON a.customer_id = c.id
ORDER BY a.balance, t.id DESC
