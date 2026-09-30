SELECT
    a.status,
    COUNT(t.id) AS transaction_count,
    COUNT(DISTINCT t.amount) AS unique_accounts,
    MAX(t.txn_time) AS last_txn_time
FROM accounts a
LEFT OUTER JOIN transactions t ON a.id = t.account_id
GROUP BY a.status
ORDER BY transaction_count DESC;
