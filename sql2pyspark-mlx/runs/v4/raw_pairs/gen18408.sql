SELECT
    merchant,
    txn_type,
    CONCAT_WS(', ', SORT_ARRAY(COLLECT_SET(merchant))) AS merchant_values,
    SUM(CASE WHEN txn_type = 'DEBIT' THEN amount ELSE 0 END) AS debit_amount
FROM transactions
WHERE is_flagged = FALSE AND (txn_type IN ('DEBIT', 'CREDIT', 'FEE') OR amount < 3600)
GROUP BY ROLLUP(merchant, txn_type)
