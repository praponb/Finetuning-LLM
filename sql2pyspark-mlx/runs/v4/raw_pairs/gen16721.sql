SELECT
    t.id,
    t.merchant,
    t.txn_type,
    a.balance
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE t.account_id = 33
ORDER BY a.balance DESC;
