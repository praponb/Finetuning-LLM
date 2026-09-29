SELECT
    a.account_type,
    COUNT(t.id) AS transaction_count,
    MIN(t.txn_time) AS first_txn_time,
    COUNT(t.amount) AS amount_count
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE t.amount < 3800
GROUP BY a.account_type
HAVING transaction_count >= 2
ORDER BY amount_count DESC, account_type
LIMIT 20
