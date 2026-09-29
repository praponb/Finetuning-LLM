SELECT
    d.account_type,
    a.txn_type,
    COUNT(*) AS rows_total,
    MAX(a.amount) AS max_amount
FROM transactions a
LEFT JOIN accounts d ON a.account_id = d.id
GROUP BY d.account_type, a.txn_type
