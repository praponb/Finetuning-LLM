SELECT
    txn_type,
    COUNT(*) cnt,
    SUM(CASE WHEN merchant = 'Shell' THEN 1 ELSE 0 END) shell_count,
    SUM(CASE WHEN txn_type = 'FEE' THEN amount ELSE 0 END) fee_amount
FROM transactions
GROUP BY txn_type
ORDER BY txn_type
LIMIT 3;
