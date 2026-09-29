SELECT a.status, COUNT(t.amount) AS amount_count
FROM transactions t
INNER JOIN accounts a ON a.id = t.account_id
GROUP BY a.status
