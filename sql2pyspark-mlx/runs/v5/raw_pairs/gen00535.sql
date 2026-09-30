SELECT
    merchant,
    account_id,
    txn_type,
    SUM(CASE WHEN merchant = '7-Eleven' THEN 1 ELSE 0 END) AS 7_eleven_count
FROM transactions
WHERE (amount >= 2800 AND HOUR(txn_time) BETWEEN 8 AND 12) OR account_id IN (4, 6, 34)
GROUP BY merchant, account_id, txn_type
