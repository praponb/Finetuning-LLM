WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        is_flagged,
        UPPER(merchant) AS merchant_upper
    FROM transactions
    WHERE merchant IS NOT NULL
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        is_flagged,
        merchant_upper,
        COUNT(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_count_amount,
        MAX(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_max_amount,
        AVG(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_avg_amount,
        MIN(amount) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_min_amount,
        FIRST_VALUE(amount, TRUE) OVER (PARTITION BY txn_type ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount
    FROM filtered
    WHERE txn_time < '2025-04-18'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        COUNT(running_min_amount) OVER w_hist AS last7_count_running_min_amount,
        MAX(running_min_amount) OVER w_hist AS last7_max_running_min_amount,
        MIN(running_min_amount) OVER w_hist AS last7_min_running_min_amount
    FROM scored
    WINDOW w_hist AS (ORDER BY running_min_amount, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
),
matched_keys AS (
    -- Set difference on keys
    SELECT id
    FROM with_windows
    EXCEPT
    SELECT id
    FROM transactions
    WHERE account_id IN (5, 6, 32)
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    JOIN matched_keys k ON s.id = k.id
)
-- Multi-level aggregation
SELECT
    CASE WHEN last7_max_running_min_amount >= 4200 THEN 'LARGE' ELSE 'SMALL' END AS last7_max_running_min_amount_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    APPROX_PERCENTILE(running_min_amount, ARRAY(0.25, 0.5, 0.75)) AS running_min_amount_quartiles,
    ROUND(AVG(running_max_amount), 2) AS avg_running_max_amount
FROM verified
GROUP BY CUBE (CASE WHEN last7_max_running_min_amount >= 4200 THEN 'LARGE' ELSE 'SMALL' END)
HAVING avg_running_max_amount >= 0
ORDER BY last7_max_running_min_amount_tier
