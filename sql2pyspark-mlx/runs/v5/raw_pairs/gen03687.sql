WITH base AS (
    SELECT
        *,
        DATE_FORMAT(start_date, 'yyyy-MM-dd') start_date_label,
        CASE WHEN status = 'CANCELLED' THEN 1 ELSE 0 END is_cancelled
    FROM subscriptions
    WHERE start_date >= DATE '2023-09-30'
      AND customer_id IS NOT NULL
),
enriched AS (
    SELECT
        *,
        AVG(is_cancelled) OVER w_hist running_avg_is_cancelled,
        MAX(is_cancelled) OVER w_hist running_max_is_cancelled,
        SUM(is_cancelled) OVER w_hist running_sum_is_cancelled,
        MIN(is_cancelled) OVER w_hist running_min_is_cancelled
    FROM base
    WHERE customer_id = 17
    WINDOW w_hist AS (PARTITION BY status ORDER BY start_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status, plan ORDER BY end_date DESC NULLS LAST, id) AS row_num
        FROM enriched
    ) t
    WHERE row_num = 1
),
cleaned AS (
    SELECT
        id,
        status,
        start_date_label,
        is_cancelled,
        running_max_is_cancelled,
        running_min_is_cancelled,
        running_avg_is_cancelled - running_sum_is_cancelled running_avg_is_cancelled_minus_running_sum_is_cancelled
    FROM latest
    WHERE customer_id IS NOT NULL
      AND MONTH(start_date) IN (4, 8, 12)
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY running_avg_is_cancelled_minus_running_sum_is_cancelled DESC, id) AS row_num
    FROM cleaned
    QUALIFY row_num = 1
),
with_windows AS (
    SELECT
        id,
        status,
        DENSE_RANK() OVER w_part running_avg_is_cancelled_minus_running_sum_is_cancelled_dense_rank,
        LEAD(running_min_is_cancelled, 1) OVER (PARTITION BY status ORDER BY running_min_is_cancelled, id) next_running_min_is_cancelled
    FROM ranked
    WINDOW w_part AS (PARTITION BY status ORDER BY running_avg_is_cancelled_minus_running_sum_is_cancelled)
),
valid AS (
    SELECT
        *,
        ROUND(next_running_min_is_cancelled * 1.07, 2) adjusted_next_running_min_is_cancelled,
        CASE WHEN next_running_min_is_cancelled >= 65 THEN 'High' WHEN next_running_min_is_cancelled >= 33 THEN 'Medium' ELSE 'Low' END next_running_min_is_cancelled_level
    FROM with_windows
    WHERE status NOT IN ('CANCELLED', 'ACTIVE')
)
SELECT
    id,
    status,
    running_avg_is_cancelled_minus_running_sum_is_cancelled_dense_rank,
    next_running_min_is_cancelled_level
FROM valid
