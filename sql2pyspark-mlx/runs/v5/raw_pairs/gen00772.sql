SELECT
    txn_type,
    MIN(txn_time) AS first_txn_time,
    SUM(CASE WHEN txn_type = 'REFUND' THEN 1 ELSE 0 END) AS refund_count,
    MAX(amount) AS max_amount
FROM transactions
WHERE merchant IS NOT NULL AND txn_time >= '2025-03-02'
GROUP BY txn_type
ORDER BY refund_count DESC, txn_type
LIMIT 5
