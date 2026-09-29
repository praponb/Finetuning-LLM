SELECT a.account_type, COUNT(t.id) AS transaction_count
FROM accounts a
LEFT JOIN transactions t ON a.id = t.account_id
GROUP BY a.account_type;
