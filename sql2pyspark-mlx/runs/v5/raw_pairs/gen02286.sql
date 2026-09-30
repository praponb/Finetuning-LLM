SELECT CASE WHEN amount >= 4100 THEN 'high' WHEN amount >= 1700 THEN 'mid' ELSE 'low' END AS amount_bucket, COUNT(amount) AS amount_count, MAX(txn_time) AS latest_txn_time
FROM transactions
GROUP BY 1
ORDER BY amount_bucket
LIMIT 5;
