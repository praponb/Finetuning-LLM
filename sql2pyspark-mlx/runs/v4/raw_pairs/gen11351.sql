SELECT
    t.account_id,
    t.txn_type,
    a.balance,
    a.opened_date,
    c.segment
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
LEFT OUTER JOIN customers c ON a.customer_id = c.id
ORDER BY balance, 2
