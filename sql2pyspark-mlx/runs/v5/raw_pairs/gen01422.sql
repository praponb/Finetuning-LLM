SELECT
    id,
    account_id,
    txn_type,
    amount,
    txn_time,
    is_flagged
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY merchant ORDER BY txn_time DESC NULLS FIRST, id) AS row_num
    FROM transactions
) x
WHERE row_num = 1
