SELECT a.account_type, MIN(t.amount) AS min_amount
FROM transactions t
INNER JOIN accounts a ON t.account_id = a.id
WHERE t.txn_time >= '2023-12-17'
GROUP BY a.account_type;
