WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        delivered_date,
        cost,
        DATE_ADD(shipped_date, 3) AS shipped_date_plus_3d,
        COALESCE(carrier, 'DIRECT') AS carrier_filled
    FROM shipments
    WHERE cost IS NOT NULL
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        LEAD(cost) OVER w_hist AS next_cost,
        LAG(cost) OVER w_hist AS prev_cost,
        ROW_NUMBER() OVER w_hist AS row_seq,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date_plus_3d, id) AS row_seq_2
    FROM base
    WINDOW w_hist AS (PARTITION BY order_id ORDER BY delivered_date, id)
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY row_seq_2 DESC NULLS LAST, id) = 1
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        row_seq,
        AVG(prev_cost) OVER (PARTITION BY order_id ORDER BY next_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_avg_prev_cost,
        COUNT(prev_cost) OVER (PARTITION BY order_id ORDER BY next_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_count_prev_cost,
        MIN(prev_cost) OVER (PARTITION BY order_id ORDER BY next_cost, id ROWS BETWEEN 3 PRECEDING AND CURRENT ROW) AS last4_min_prev_cost,
        RANK() OVER (PARTITION BY order_id ORDER BY row_seq DESC) AS row_seq_rank
    FROM ranked
    WHERE cost >= 36
),
with_windows_shipments AS (
    -- Running and moving aggregates
    SELECT
        id,
        row_seq,
        last4_count_prev_cost,
        last4_min_prev_cost,
        ROW_NUMBER() OVER (ORDER BY last4_count_prev_cost DESC, id) AS row_seq_2,
        LAG(last4_min_prev_cost, 1) OVER (ORDER BY last4_avg_prev_cost, id) AS prev_last4_min_prev_cost
    FROM scored
)
-- Multi-level aggregation
SELECT
    CASE WHEN last4_min_prev_cost >= 87 THEN 'LARGE' ELSE 'SMALL' END AS last4_min_prev_cost_tier,
    MAX(row_seq) AS max_row_seq,
    PERCENTILE_APPROX(row_seq_2, 0.9) AS row_seq_2_p90,
    ROUND(AVG(row_seq_2), 2) AS avg_row_seq_2
FROM with_windows_shipments
GROUP BY CASE WHEN last4_min_prev_cost >= 87 THEN 'LARGE' ELSE 'SMALL' END;
