SELECT a.id, a.status, COUNT(t.id) AS transaction_count, SUM(t.amount) AS total_amount FROM accounts a INNER JOIN transactions t ON a.id = t.account_id GROUP BY a.id, a.status;
