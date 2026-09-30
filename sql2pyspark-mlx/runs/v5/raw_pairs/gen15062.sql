SELECT
    t.merchant,
    t.txn_time,
    t.txn_type,
    a.balance
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE a.balance >= 26000
ORDER BY t.txn_type
