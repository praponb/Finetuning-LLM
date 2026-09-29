SELECT
    t.txn_type,
    t.txn_time,
    t.is_flagged,
    a.balance,
    a.account_type
FROM transactions t
JOIN accounts a ON a.id = t.account_id
