WITH base AS (
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        HOUR(txn_time) AS txn_time_hour
    FROM transactions
    WHERE txn_type = 'DEBIT'
)
SELECT
    id,
    account_id,
    txn_type,
    txn_time_hour
FROM (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY amount NULLS LAST, id) AS dedup_rank
    FROM base
) ranked
WHERE dedup_rank = 1;
