SELECT
    merchant,
    account_id,
    txn_time,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE HOUR(txn_time) BETWEEN 6 AND 13 OR amount BETWEEN 1200 AND 3300
ORDER BY txn_time;
