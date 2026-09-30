WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CAST(amount AS INT) AS amount_int,
        COALESCE(method, 'NONE') AS resolved_method
    FROM payments
    WHERE order_id IS NOT NULL
      AND method IS NOT DISTINCT FROM 'cash'
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        LAST_VALUE(amount_int) IGNORE NULLS OVER w_part AS last_known_amount_int,
        AVG(amount_int) OVER (PARTITION BY order_id, method) AS order_id_avg_amount_int
    FROM base
    WINDOW w_part AS (ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
ranked AS (
    -- Deduplicate with ROW_NUMBER
    SELECT
        id,
        order_id,
        paid_at,
        status,
        amount_int
    FROM (
        SELECT *, RANK() OVER (PARTITION BY status, method ORDER BY amount_int DESC) AS latest_rank
        FROM metrics
    ) x
    WHERE latest_rank = 1
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(amount_int) OVER w_part AS running_min_amount_int,
        COUNT(amount_int) OVER w_part AS running_count_amount_int,
        SUM(amount_int) OVER w_part AS running_sum_amount_int,
        NTILE(3) OVER w_hist AS amount_int_bucket
    FROM ranked
    WHERE paid_at < '2025-09-22'
    WINDOW w_part AS (PARTITION BY status ORDER BY amount_int, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW), w_hist AS (PARTITION BY status ORDER BY amount_int, id)
),
summary AS (
    -- Grouped metrics
    SELECT
        order_id,
        PERCENTILE_APPROX(running_count_amount_int, ARRAY(0.25, 0.5, 0.75)) AS running_count_amount_int_quartiles,
        SIZE(COLLECT_SET(status)) AS num_status,
        COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refunded_rows,
        COUNT(DISTINCT status) AS distinct_status,
        GROUPING(order_id) AS grouping_id
    FROM with_windows
    GROUP BY CUBE (order_id)
)
SELECT num_status, refunded_rows
FROM summary
ORDER BY refunded_rows NULLS FIRST
