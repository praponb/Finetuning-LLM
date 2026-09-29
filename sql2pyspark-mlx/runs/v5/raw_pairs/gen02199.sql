SELECT
    t.id,
    t.amount,
    t.txn_type,
    t.account_id,
    a.balance
FROM transactions t
LEFT OUTER JOIN accounts a ON t.account_id = a.id
ORDER BY account_id DESC;
