SELECT a.account_type, COUNT(t.amount) AS amount_count FROM transactions t LEFT JOIN accounts a ON t.account_id = a.id GROUP BY a.account_type ORDER BY account_type DESC;
