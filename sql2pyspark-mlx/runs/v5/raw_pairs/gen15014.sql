WITH ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY txn_time, id) AS row_num
        FROM transactions
    ) ranked
    WHERE row_num = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id) prev_amount,
        LEAD(amount) OVER (PARTITION BY account_id ORDER BY txn_time, id) next_amount,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY txn_time, id) seq_no
    FROM ranked
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        ROUND(seq_no / next_amount, 2) seq_no_per_next_amount,
        COALESCE(seq_no, 0) seq_no_filled,
        COALESCE(merchant, 'N/A') resolved_merchant
    FROM metrics
    WHERE txn_time > DATE '2023-09-18'
      AND is_flagged
),
latest AS (
    -- Deduplicate with ROW_NUMBER
    SELECT *
    FROM base
    QUALIFY ROW_NUMBER() OVER (PARTITION BY txn_type, account_id ORDER BY row_num DESC, id) = 1
)
SELECT
    id,
    txn_type,
    amount,
    row_num,
    prev_amount,
    next_amount,
    seq_no,
    seq_no_per_next_amount,
    seq_no_filled
FROM latest
ORDER BY txn_type DESC NULLS FIRST, id
LIMIT 5;
