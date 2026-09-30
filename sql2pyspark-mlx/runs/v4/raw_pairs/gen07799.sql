SELECT
    t.id,
    t.account_id,
    t.txn_type,
    t.is_flagged,
    a.opened_date,
    a.status
FROM transactions t
LEFT JOIN accounts a ON t.account_id = a.id
WHERE t.amount > 1500;
