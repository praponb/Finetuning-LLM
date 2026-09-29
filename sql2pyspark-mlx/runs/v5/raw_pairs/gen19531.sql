WITH valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CAST(amount AS INT) AS amount_int,
        COALESCE(merchant, 'NONE') AS merchant_filled
    FROM transactions
    WHERE TO_DATE(txn_time) < '2024-12-10'
),
with_gaps AS (
    -- Flag rows that start a new session (gap > interval)
    SELECT *, CASE WHEN LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) IS NULL
                 OR txn_time > LAG(txn_time) OVER (PARTITION BY txn_type ORDER BY txn_time, id) + INTERVAL '14' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM valid
),
islands AS (
    -- Running sum of the flag = session number
    SELECT
        id,
        txn_type,
        amount,
        txn_time,
        merchant,
        is_flagged,
        merchant_filled,
        SUM(session_start_flag) OVER (PARTITION BY txn_type ORDER BY txn_time, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM with_gaps
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        txn_type,
        amount,
        is_flagged,
        merchant_filled,
        NTILE(10) OVER (ORDER BY amount DESC, id) AS amount_bucket,
        ROW_NUMBER() OVER (ORDER BY amount, id) AS row_seq,
        SUM(session_id) OVER (PARTITION BY txn_type, merchant ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_session_id
    FROM islands
),
metrics_transactions AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        is_flagged,
        amount_bucket,
        MAX(amount_bucket) OVER (PARTITION BY txn_type ORDER BY amount_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_amount_bucket,
        MIN(amount_bucket) OVER (PARTITION BY txn_type ORDER BY amount_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_amount_bucket,
        COUNT(amount_bucket) OVER (PARTITION BY txn_type ORDER BY amount_bucket, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_count_amount_bucket
    FROM metrics
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(last7_min_amount_bucket) OVER (ORDER BY last7_max_amount_bucket, id ROWS BETWEEN 5 PRECEDING AND 1 PRECEDING) AS last7_min_amount_bucket_avg_5_prior,
        SUM(amount_bucket) OVER (ORDER BY amount_bucket, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_amount_bucket
    FROM metrics_transactions
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN last7_max_amount_bucket >= 8.8 THEN 'LARGE' ELSE 'SMALL' END AS last7_max_amount_bucket_tier,
    CASE WHEN cumulative_amount_bucket >= 38 THEN 'HIGH' ELSE 'LOW' END AS cumulative_amount_bucket_tier,
    MIN(amount_bucket) AS min_amount_bucket,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(last7_max_amount_bucket) AS total_last7_max_amount_bucket,
    GROUPING_ID() AS gid
FROM enriched
GROUP BY ROLLUP (CASE WHEN last7_max_amount_bucket >= 8.8 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN cumulative_amount_bucket >= 38 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY cumulative_amount_bucket_tier DESC NULLS LAST
