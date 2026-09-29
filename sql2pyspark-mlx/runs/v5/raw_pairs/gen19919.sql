SELECT
    a.balance,
    a.account_type,
    a.opened_date,
    t.txn_time,
    t.account_id
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
ORDER BY txn_time
