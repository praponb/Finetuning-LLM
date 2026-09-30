SELECT a.account_type, COUNT(t.id) AS transaction_count, COUNT(DISTINCT t.amount) AS unique_accounts
FROM accounts a
INNER JOIN transactions t ON a.id = t.account_id
GROUP BY a.account_type
ORDER BY a.account_type DESC
LIMIT 5
