SELECT
    a.opened_date,
    a.status,
    a.customer_id,
    t.txn_time
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id;
