SELECT
    id,
    account_id,
    amount,
    txn_time,
    merchant,
    is_flagged
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time DESC, id) AS dedup_rank
    FROM transactions
) t
WHERE dedup_rank = 1;
