SELECT DATE_TRUNC('MONTH', txn_time) AS txn_month, MIN(txn_time) AS first_txn_time, MAX(amount) AS max_amount
FROM transactions
WHERE txn_type IN ('REFUND', 'CREDIT') AND account_id IN (8, 18) AND account_id IN (7, 17, 25, 35)
GROUP BY DATE_TRUNC('MONTH', txn_time)
HAVING MAX(amount) > 1500
ORDER BY first_txn_time;
