SELECT
    id,
    txn_time,
    txn_type,
    account_id
FROM transactions
WHERE is_flagged
ORDER BY txn_type;
