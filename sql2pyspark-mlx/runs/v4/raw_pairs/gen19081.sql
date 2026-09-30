SELECT
    merchant,
    MIN(txn_time) AS min_txn_time,
    COUNT(CASE WHEN merchant = '7-Eleven' THEN 1 END) AS 7_eleven_count,
    SUM(CASE WHEN txn_type = 'CREDIT' THEN 1 ELSE 0 END) AS credit_count
FROM transactions
WHERE txn_type IN ('REFUND', 'FEE', 'DEBIT')
GROUP BY 1
