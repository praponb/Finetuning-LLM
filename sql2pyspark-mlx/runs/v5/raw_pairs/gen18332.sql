SELECT a.id, a.account_type, COUNT(t.id) AS transaction_count FROM accounts a INNER JOIN transactions t ON a.id = t.account_id GROUP BY a.id, a.account_type HAVING COUNT(t.id) >= 1
