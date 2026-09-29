SELECT txn_type, account_id, ROUND(AVG(amount), 2) AS avg_amount
FROM transactions
GROUP BY txn_type, account_id
HAVING ROUND(AVG(amount), 2) < 52
