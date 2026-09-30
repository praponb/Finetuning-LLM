SELECT a.id, a.account_type, COUNT(t.id) AS transaction_count
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
WHERE a.status IN ('OPEN', 'FROZEN', 'CLOSED')
GROUP BY a.id, a.account_type
