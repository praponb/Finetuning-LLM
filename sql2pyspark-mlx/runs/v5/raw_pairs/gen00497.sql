WITH cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        method,
        status,
        CASE WHEN amount >= 1410 THEN 'High' WHEN amount >= 700 THEN 'Medium' ELSE 'Low' END AS amount_tier
    FROM payments
    WHERE status NOT IN ('SUCCESS', 'REFUNDED')
      AND method IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(method) AS method_length,
        LOWER(status) AS status_lower
    FROM cleaned
    WHERE method IS NOT NULL
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(method_length) OVER w_hist AS running_sum_method_length,
        AVG(method_length) OVER w_hist AS running_avg_method_length,
        COUNT(method_length) OVER w_hist AS running_count_method_length,
        ROW_NUMBER() OVER (PARTITION BY status ORDER BY method_length, id) AS event_seq
    FROM valid
    WINDOW w_hist AS (PARTITION BY method ORDER BY method_length, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        method,
        status,
        amount_tier,
        status_lower,
        running_sum_method_length,
        event_seq,
        CONCAT(method, ', ', status) AS method_status,
        COALESCE(method, 'N/A') AS method_clean
    FROM metrics
    WHERE method IS NOT NULL
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        LAG(running_sum_method_length, 1) OVER (ORDER BY running_sum_method_length, id) AS prev_running_sum_method_length,
        NTILE(5) OVER (ORDER BY running_sum_method_length, id) AS running_sum_method_length_bucket
    FROM filtered
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT
        id,
        method,
        status,
        amount_tier,
        status_lower,
        method_clean,
        running_sum_method_length_bucket
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY event_seq DESC, id) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
)
-- Multi-level aggregation
SELECT
    method,
    CASE WHEN running_sum_method_length_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END AS running_sum_method_length_bucket_tier,
    COUNT(CASE WHEN method = 'card' THEN 1 END) AS card_count,
    SUM(CASE WHEN status = 'SUCCESS' THEN running_sum_method_length_bucket ELSE 0 END) AS success_running_sum_method_length_bucket,
    APPROX_PERCENTILE(running_sum_method_length_bucket, 0.9) AS running_sum_method_length_bucket_p90,
    ROUND(SUM(running_sum_method_length_bucket) / NULLIF(COUNT(CASE WHEN status = 'SUCCESS' THEN 1 END), 0), 2) AS running_sum_method_length_bucket_per_success,
    SUM(running_sum_method_length_bucket) AS total_running_sum_method_length_bucket
FROM ranked
GROUP BY method, CASE WHEN running_sum_method_length_bucket >= 8 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY running_sum_method_length_bucket_p90 DESC;
