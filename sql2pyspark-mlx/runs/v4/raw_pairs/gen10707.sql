SELECT a.id, a.status, COUNT(t.id) AS transaction_count
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
WHERE TO_DATE(t.txn_time) >= '2025-01-01'
GROUP BY a.id, a.status
