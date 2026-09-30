SELECT
    a.customer_id,
    a.status,
    t.amount,
    t.account_id,
    t.txn_time
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE a.customer_id = 29;
