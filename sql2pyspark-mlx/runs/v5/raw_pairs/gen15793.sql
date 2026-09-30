SELECT
    a.customer_id,
    a.status,
    a.account_type,
    t.account_id,
    t.txn_time,
    t.merchant
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
