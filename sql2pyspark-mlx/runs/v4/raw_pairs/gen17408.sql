WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        merchant,
        is_flagged,
        SUBSTRING(txn_type, 1, 3) txn_type_prefix
    FROM transactions
    WHERE merchant IS NOT NULL
      AND merchant != 'Grab'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_type_prefix,
        FLOOR(amount) amount_whole
    FROM base
    WHERE merchant NOT IN ('Lazada', 'Starbucks')
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(amount_whole) OVER w_grp running_amount_whole,
        RANK() OVER (ORDER BY amount_whole DESC) amount_whole_rank
    FROM valid
    WINDOW w_grp AS (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        NTILE(3) OVER (PARTITION BY account_id, txn_type ORDER BY amount DESC, id) amount_bucket,
        LAG(amount_whole, 1) OVER (ORDER BY amount_whole_rank, id) prev_amount_whole,
        ROUND(amount / SUM(amount) OVER (PARTITION BY account_id, txn_type), 4) amount_share
    FROM enriched
),
enriched_transactions AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(amount_bucket) IGNORE NULLS OVER w_running first_known_amount_bucket,
        FIRST_VALUE(running_amount_whole) IGNORE NULLS OVER w_running_2 first_known_running_amount_whole
    FROM with_windows
    WINDOW w_running AS (PARTITION BY txn_type ORDER BY prev_amount_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_running_2 AS (PARTITION BY account_id ORDER BY running_amount_whole, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Grouped metrics
SELECT
    txn_type,
    COUNT(*) AS event_count,
    APPROX_PERCENTILE(amount_whole_rank, 0.95) AS amount_whole_rank_p95
FROM enriched_transactions
GROUP BY 1
HAVING amount_whole_rank_p95 < 0
ORDER BY amount_whole_rank_p95 DESC NULLS FIRST;
