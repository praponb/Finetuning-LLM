SELECT a.id, COUNT(t.id) AS transaction_count FROM accounts a JOIN transactions t ON a.id = t.account_id GROUP BY a.id ORDER BY id DESC
