WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        DATE_TRUNC('WEEK', txn_time) AS txn_time_period,
        DATE_FORMAT(txn_time, 'MMM yyyy') AS txn_time_label
    FROM transactions
    WHERE merchant IS NOT NULL
      AND txn_type = 'DEBIT'
)
-- Deduplicate with ROW_NUMBER
SELECT
    id,
    txn_type,
    amount,
    merchant,
    is_flagged,
    txn_time_period,
    txn_time_label
FROM valid
QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time ASC NULLS LAST, id ASC) = 1
