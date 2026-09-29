WITH base AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LOWER(status) AS status_lower,
        COALESCE(method, 'N/A') AS method_filled
    FROM payments
    WHERE paid_at >= TO_TIMESTAMP('2023-09-27 03:00:00')
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        order_id,
        method,
        amount,
        method_filled,
        MAX(amount) OVER w_hist AS running_max_amount,
        AVG(amount) OVER w_hist AS running_avg_amount,
        MIN(amount) OVER w_hist AS running_min_amount,
        SUM(amount) OVER w_hist AS running_sum_amount,
        NTILE(3) OVER (PARTITION BY order_id, status ORDER BY amount, id) AS amount_bucket
    FROM base
    WINDOW w_hist AS (PARTITION BY method, order_id ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
kept_keys AS (
    -- Set difference on keys
    SELECT id, method
    FROM with_windows
    EXCEPT ALL
    SELECT id, method
    FROM payments
    WHERE TO_DATE(paid_at) < '2024-02-01'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN kept_keys k ON s.id = k.id AND s.method = k.method
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        order_id,
        method,
        amount,
        running_max_amount,
        running_min_amount,
        DENSE_RANK() OVER (ORDER BY amount_bucket DESC NULLS LAST) AS amount_bucket_dense_rank,
        AVG(amount_bucket) OVER (PARTITION BY method, order_id) AS method_avg_amount_bucket
    FROM verified
)
-- Aggregate with conditional metrics
SELECT
    order_id,
    method,
    PERCENTILE_APPROX(running_max_amount, ARRAY(0.25, 0.5, 0.75)) AS running_max_amount_quartiles,
    ROUND(SUM(method_avg_amount_bucket) / NULLIF(COUNT(CASE WHEN method = 'cash' THEN 1 END), 0), 2) AS method_avg_amount_bucket_per_cash,
    APPROX_PERCENTILE(amount, 0.5) AS amount_p50,
    COUNT(DISTINCT order_id) AS distinct_order_ids
FROM metrics
GROUP BY GROUPING SETS ((order_id, method), (order_id), ())
HAVING APPROX_PERCENTILE(amount, 0.5) > 0
ORDER BY distinct_order_ids DESC NULLS LAST;
