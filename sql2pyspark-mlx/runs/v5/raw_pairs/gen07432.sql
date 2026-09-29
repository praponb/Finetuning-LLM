SELECT
    DATE_TRUNC('MONTH', txn_time) AS txn_month,
    txn_type,
    COUNT(*) AS num_transactions,
    MAX(amount) - MIN(amount) AS amount_range,
    MAX(txn_time) AS latest_txn_time
FROM transactions
GROUP BY DATE_TRUNC('MONTH', txn_time), txn_type
ORDER BY amount_range, latest_txn_time, txn_month, txn_type
LIMIT 50
