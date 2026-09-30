WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        IF(is_flagged, 'Yes', 'No') AS is_flagged_label
    FROM transactions
    WHERE txn_time > TIMESTAMP '2024-01-23 11:00:00'
      AND merchant IS NOT NULL
)
-- Window metrics over several different partitions
SELECT
    *,
    LEAD(amount, 1) OVER (PARTITION BY merchant, txn_type ORDER BY amount ASC, id ASC) AS next_amount,
    LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY txn_type ORDER BY amount ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
    NTILE(3) OVER (PARTITION BY txn_type ORDER BY amount DESC, id ASC) AS amount_bucket
FROM base;
