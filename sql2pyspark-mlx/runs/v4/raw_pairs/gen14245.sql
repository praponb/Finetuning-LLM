SELECT
    id,
    txn_type,
    merchant,
    is_flagged
FROM transactions
WHERE NOT is_flagged
