WITH with_windows AS (
    -- Ranking and gap features
    SELECT
        id,
        account_id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        FIRST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY txn_type, account_id ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        MAX(amount) OVER (PARTITION BY merchant, txn_type ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_max_30d
    FROM transactions
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(first_known_amount / SUM(first_known_amount) OVER (PARTITION BY merchant), 4) AS first_known_amount_share,
        AVG(first_known_amount) OVER (PARTITION BY merchant ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS first_known_amount_avg_30d,
        COUNT(amount_max_30d) OVER (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW) AS amount_max_30d_count_30d,
        PERCENT_RANK() OVER (PARTITION BY merchant ORDER BY amount_max_30d NULLS FIRST) AS amount_max_30d_percent_rank,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY amount_max_30d DESC, id) AS event_seq
    FROM with_windows
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        txn_type,
        merchant,
        first_known_amount,
        first_known_amount_share,
        first_known_amount_avg_30d,
        amount_max_30d_count_30d,
        amount_max_30d_percent_rank,
        event_seq,
        CAST(amount AS INT) AS amount_int
    FROM scored
    WHERE event_seq < 4
),
kept_keys AS (
    -- Set difference on keys
    SELECT merchant, id
    FROM filtered
    EXCEPT ALL
    SELECT merchant, id
    FROM transactions
    WHERE is_flagged = FALSE
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM filtered s
    INNER JOIN kept_keys k ON s.merchant = k.merchant AND s.id = k.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        merchant,
        first_known_amount,
        first_known_amount_share,
        first_known_amount_avg_30d,
        amount_max_30d_count_30d,
        amount_max_30d_percent_rank,
        event_seq,
        amount_int,
        ROUND(amount_max_30d_count_30d / event_seq, 2) AS amount_max_30d_count_30d_per_event_seq,
        COALESCE(merchant, 'DIRECT') AS merchant_filled
    FROM reconciled
    WHERE amount_int BETWEEN 42 AND 67
)
-- Multi-level aggregation
SELECT
    CASE WHEN first_known_amount_share >= 0.76 THEN 'PREMIUM' ELSE 'STANDARD' END AS first_known_amount_share_tier,
    merchant,
    MIN(first_known_amount_avg_30d) AS min_first_known_amount_avg_30d,
    SUM(amount_int) FILTER (WHERE merchant = 'Shell') AS shell_total_amount_int,
    SORT_ARRAY(COLLECT_SET(merchant)) AS merchant_list
FROM valid
GROUP BY first_known_amount_share_tier, merchant
ORDER BY merchant DESC NULLS LAST;
