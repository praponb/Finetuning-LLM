SELECT
    a.status,
    a.opened_date,
    t.amount,
    t.txn_time
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE a.customer_id IS NOT NULL
ORDER BY a.status DESC, a.opened_date
