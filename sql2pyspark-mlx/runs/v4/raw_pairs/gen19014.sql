SELECT a.status, COUNT(*) AS transaction_count
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
GROUP BY a.status;
