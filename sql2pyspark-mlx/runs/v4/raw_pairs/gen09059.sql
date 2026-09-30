SELECT
    a.account_type,
    a.status,
    a.opened_date,
    t.account_id
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE TO_DATE(t.txn_time) >= '2024-02-01'
ORDER BY t.account_id DESC;
