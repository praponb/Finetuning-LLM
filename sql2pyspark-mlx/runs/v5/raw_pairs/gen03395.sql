WITH ranked AS (
    SELECT *
    FROM shipments
    QUALIFY ROW_NUMBER() OVER (PARTITION BY carrier, order_id ORDER BY cost DESC, id) = 1
),
with_windows AS (
    SELECT
        id,
        order_id,
        carrier,
        shipped_date,
        delivered_date,
        cost,
        MAX(cost) OVER (PARTITION BY status, order_id) status_max_cost,
        FIRST_VALUE(carrier) IGNORE NULLS OVER w_ordered first_known_carrier,
        AVG(cost) OVER (PARTITION BY carrier ORDER BY shipped_date, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) cost_avg_6,
        LAG(cost, 1) OVER (PARTITION BY carrier ORDER BY cost, id) prev_cost,
        LAG(cost, 1, 0) OVER (PARTITION BY status ORDER BY shipped_date, id) prev_cost_2
    FROM ranked
    WINDOW w_ordered AS (PARTITION BY carrier ORDER BY delivered_date, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
latest AS (
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id, carrier ORDER BY prev_cost, id) = 1
),
scored AS (
    SELECT
        id,
        delivered_date,
        cost,
        cost_avg_6,
        prev_cost,
        prev_cost_2,
        SUM(prev_cost_2) OVER w_ordered running_sum_prev_cost_2,
        AVG(prev_cost_2) OVER w_ordered running_avg_prev_cost_2,
        MIN(prev_cost_2) OVER w_ordered running_min_prev_cost_2
    FROM latest
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY status_max_cost, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
base AS (
    SELECT
        id,
        delivered_date,
        cost_avg_6,
        prev_cost,
        prev_cost_2,
        running_sum_prev_cost_2,
        running_min_prev_cost_2,
        WEEKOFYEAR(delivered_date) delivered_date_weekofyear
    FROM scored
    WHERE cost_avg_6 = 61
),
filtered AS (
    SELECT
        id,
        delivered_date,
        cost_avg_6,
        prev_cost,
        prev_cost_2,
        running_sum_prev_cost_2,
        running_min_prev_cost_2,
        delivered_date_weekofyear,
        ROUND(running_sum_prev_cost_2, 0) running_sum_prev_cost_2_rounded,
        CAST(running_sum_prev_cost_2 AS INT) running_sum_prev_cost_2_int
    FROM base
    WHERE delivered_date >= TO_DATE('2023-03-17')
)
SELECT
    DATE_TRUNC('WEEK', delivered_date) AS week_start,
    DATE_TRUNC('MONTH', delivered_date) AS activity_month,
    DATE_TRUNC('QUARTER', delivered_date) AS quarter_start,
    PERCENTILE_APPROX(delivered_date_weekofyear, 0.95) AS delivered_date_weekofyear_p95,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(running_sum_prev_cost_2_int), 2) AS avg_running_sum_prev_cost_2_int,
    MAX(running_sum_prev_cost_2_int) AS max_running_sum_prev_cost_2_int,
    AVG(delivered_date_weekofyear) AS avg_delivered_date_weekofyear,
    ROUND(AVG(prev_cost_2), 2) AS avg_prev_cost_2,
    GROUPING(DATE_TRUNC('WEEK', delivered_date)) AS grouping_level
FROM filtered
GROUP BY GROUPING SETS ((DATE_TRUNC('WEEK', delivered_date), DATE_TRUNC('MONTH', delivered_date), DATE_TRUNC('QUARTER', delivered_date)), (DATE_TRUNC('WEEK', delivered_date), DATE_TRUNC('MONTH', delivered_date)), ())
ORDER BY avg_running_sum_prev_cost_2_int;
