WITH with_dims AS (
    -- Attach accounts attributes
    SELECT
        a.id,
        a.account_id,
        a.txn_type,
        a.amount,
        a.txn_time,
        d.opened_date
    FROM transactions a
    LEFT JOIN accounts d
        ON a.account_id = d.id
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS is_new_burst
    FROM with_dims
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        account_id,
        txn_type,
        txn_time,
        opened_date,
        SUM(is_new_burst) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_type,
        burst_id,
        MIN(burst_id) OVER w_part AS last3_min_burst_id,
        MAX(burst_id) OVER w_part AS last3_max_burst_id,
        SUM(burst_id) OVER w_part AS last3_sum_burst_id,
        AVG(burst_id) OVER w_part AS last3_avg_burst_id
    FROM islands
    WHERE burst_id BETWEEN 4 AND 5
    WINDOW w_part AS (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        txn_type,
        burst_id,
        last3_avg_burst_id
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY txn_type ORDER BY last3_sum_burst_id DESC NULLS LAST, id) AS rn
        FROM with_windows
    ) ranked
    WHERE rn = 1
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        txn_type,
        burst_id,
        last3_avg_burst_id,
        LEAD(last3_avg_burst_id) OVER w_running AS next_last3_avg_burst_id,
        LEAD(burst_id) OVER w_running AS next_burst_id,
        ROW_NUMBER() OVER w_running AS seq_no,
        AVG(last3_avg_burst_id) OVER (PARTITION BY txn_type ORDER BY last3_avg_burst_id, id ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) AS last3_avg_burst_id_avg_2
    FROM latest
    WINDOW w_running AS (PARTITION BY txn_type ORDER BY last3_avg_burst_id, id)
),
with_windows_transactions AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAG(last3_avg_burst_id, 1) OVER (PARTITION BY txn_type ORDER BY last3_avg_burst_id_avg_2, id) AS prev_last3_avg_burst_id,
        CUME_DIST() OVER (PARTITION BY txn_type ORDER BY burst_id) AS burst_id_cume_dist
    FROM metrics
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        burst_id,
        last3_avg_burst_id,
        next_burst_id,
        prev_last3_avg_burst_id,
        burst_id_cume_dist,
        AVG(prev_last3_avg_burst_id) OVER w_part AS running_avg_prev_last3_avg_burst_id,
        MIN(prev_last3_avg_burst_id) OVER w_part AS running_min_prev_last3_avg_burst_id,
        COUNT(prev_last3_avg_burst_id) OVER w_part AS running_count_prev_last3_avg_burst_id,
        SUM(prev_last3_avg_burst_id) OVER w_part AS running_sum_prev_last3_avg_burst_id,
        NTILE(10) OVER (PARTITION BY txn_type ORDER BY burst_id DESC, id) AS burst_id_bucket
    FROM with_windows_transactions
    WINDOW w_part AS (PARTITION BY txn_type ORDER BY burst_id, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN burst_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_tier,
    COUNT(*) AS cnt,
    PERCENTILE_APPROX(running_sum_prev_last3_avg_burst_id, 0.5) AS running_sum_prev_last3_avg_burst_id_p50,
    PERCENTILE_APPROX(burst_id, ARRAY(0.25, 0.5, 0.75)) AS burst_id_quartiles,
    SUM(running_avg_prev_last3_avg_burst_id) AS total_running_avg_prev_last3_avg_burst_id,
    COUNT(DISTINCT id) AS distinct_ids
FROM scored
GROUP BY CASE WHEN burst_id >= 8 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING COUNT(DISTINCT id) >= 1
ORDER BY distinct_ids;
