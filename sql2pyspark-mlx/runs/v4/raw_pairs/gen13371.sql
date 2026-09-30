SELECT
    txn_type,
    HOUR(txn_time) AS txn_hour,
    SUM(amount) AS total_amount,
    MAX(amount) AS max_amount
FROM transactions
GROUP BY txn_type, txn_hour
HAVING max_amount > 2400
