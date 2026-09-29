SELECT
    id,
    txn_type,
    is_flagged,
    CAST(is_flagged AS INT) AS is_flagged_flag
FROM transactions
WHERE amount > 3400
