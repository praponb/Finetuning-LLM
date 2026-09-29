SELECT
    t.id,
    t.txn_type,
    t.account_id,
    a.opened_date,
    a.status
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE a.balance <= 26000
ORDER BY a.status;
