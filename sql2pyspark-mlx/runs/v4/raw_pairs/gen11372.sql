SELECT CASE WHEN amount >= 4000 THEN 'high' WHEN amount >= 600 THEN 'mid' ELSE 'low' END AS amount_bucket, txn_type, MAX(amount) - MIN(amount) AS amount_range
FROM transactions
WHERE account_id = 1
GROUP BY amount_bucket, txn_type
