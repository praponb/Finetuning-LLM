WITH remaining_keys AS (
    -- Set difference on keys
    SELECT txn_type, merchant
    FROM transactions
    EXCEPT ALL
    SELECT txn_type, merchant
    FROM transactions
    WHERE amount BETWEEN 1800 AND 2900
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM transactions s
    JOIN remaining_keys k ON s.txn_type = k.txn_type AND s.merchant = k.merchant
),
scored AS (
    -- Running and moving aggregates
    SELECT
        txn_type,
        amount,
        merchant,
        is_flagged,
        AVG(amount) OVER (PARTITION BY txn_type) AS txn_type_avg_amount,
        MAX(amount) OVER w_hist AS amount_max_30d
    FROM verified
    WHERE amount > 1100
    WINDOW w_hist AS (PARTITION BY account_id ORDER BY CAST(txn_time AS BIGINT) RANGE BETWEEN 2592000 PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        txn_type,
        amount,
        is_flagged,
        txn_type_avg_amount,
        amount_max_30d,
        CAST(is_flagged AS INT) AS is_flagged_flag
    FROM scored
    WHERE merchant IS DISTINCT FROM 'Shell'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        txn_type,
        amount,
        amount_max_30d,
        is_flagged_flag,
        AVG(is_flagged_flag) OVER w_grp AS txn_type_avg_is_flagged_flag,
        COUNT(is_flagged_flag) OVER w_grp AS txn_type_count_is_flagged_flag
    FROM filtered
    WINDOW w_grp AS (PARTITION BY txn_type)
),
scored_transactions AS (
    -- Running and moving aggregates
    SELECT
        amount,
        amount_max_30d,
        txn_type_avg_is_flagged_flag,
        PERCENT_RANK() OVER (PARTITION BY txn_type ORDER BY amount NULLS LAST) AS amount_percent_rank,
        CUME_DIST() OVER (ORDER BY amount_max_30d NULLS LAST) AS amount_max_30d_cume_dist
    FROM with_windows
)
-- Multi-level aggregation
SELECT
    CASE WHEN amount_max_30d_cume_dist >= 0.82 THEN 'PREMIUM' ELSE 'STANDARD' END AS amount_max_30d_cume_dist_tier,
    MAX(txn_type_avg_is_flagged_flag) AS max_txn_type_avg_is_flagged_flag,
    PERCENTILE_APPROX(txn_type_avg_is_flagged_flag, ARRAY(0.25, 0.5, 0.75)) AS txn_type_avg_is_flagged_flag_quartiles,
    ROUND(AVG(amount), 2) AS avg_amount,
    COUNT(*) AS event_count
FROM scored_transactions
GROUP BY 1
ORDER BY avg_amount DESC NULLS FIRST
