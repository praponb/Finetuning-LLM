SELECT a.id, COUNT(t.id) transaction_count FROM accounts a LEFT JOIN transactions t ON t.account_id = a.id GROUP BY a.id HAVING COUNT(t.id) >= 4
