SELECT
    id,
    txn_type,
    txn_time,
    CAST(is_flagged AS INT) is_flagged_flag
FROM transactions
ORDER BY txn_type DESC
