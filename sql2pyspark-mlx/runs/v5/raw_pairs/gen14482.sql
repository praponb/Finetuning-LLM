SELECT a.account_type, COUNT(t.id) AS transaction_count
FROM accounts a
LEFT JOIN transactions t ON t.account_id = a.id
GROUP BY a.account_type
HAVING transaction_count > 2
