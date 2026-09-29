WITH joined AS (
    -- Attach orders attributes
    SELECT src.*, b.discount
    FROM payments src
    LEFT JOIN orders b
        ON src.order_id = b.id
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        status,
        discount,
        CASE WHEN amount >= 1540 THEN 'High' WHEN amount >= 610 THEN 'Medium' ELSE 'Low' END amount_tier,
        TRIM(status) clean_status
    FROM joined
    WHERE paid_at >= DATE '2023-05-12'
      AND order_id IS NOT NULL
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(discount) OVER (PARTITION BY order_id) order_id_stddev_samp_discount,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY discount DESC, id) event_seq,
        SUM(discount) OVER (PARTITION BY status, order_id) status_sum_discount
    FROM valid
),
ranked AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM with_windows
    QUALIFY ROW_NUMBER() OVER (PARTITION BY status, order_id ORDER BY status_sum_discount NULLS LAST, id) = 1
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        discount,
        amount_tier,
        clean_status,
        order_id_stddev_samp_discount,
        event_seq,
        status_sum_discount,
        COALESCE(discount, 0) discount_filled
    FROM ranked
    WHERE status_sum_discount = 2
),
with_windows_payments AS (
    -- Running and moving aggregates
    SELECT
        id,
        clean_status,
        order_id_stddev_samp_discount,
        event_seq,
        ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY discount_filled, id) event_seq_2,
        SUM(order_id_stddev_samp_discount) OVER (PARTITION BY order_id ORDER BY discount_filled, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_order_id_stddev_samp_discount,
        AVG(event_seq) OVER (PARTITION BY order_id ORDER BY status_sum_discount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) event_seq_avg_3_prior,
        SUM(order_id_stddev_samp_discount) OVER (PARTITION BY order_id ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_order_id_stddev_samp_discount_2
    FROM base
)
-- Grouped metrics
SELECT
    CASE WHEN running_order_id_stddev_samp_discount_2 >= 2 THEN 'LARGE' ELSE 'SMALL' END AS running_order_id_stddev_samp_discount_2_tier,
    APPROX_PERCENTILE(event_seq, 0.5) AS event_seq_p50,
    APPROX_PERCENTILE(order_id_stddev_samp_discount, 0.5) AS order_id_stddev_samp_discount_p50,
    COUNT(*) AS event_count
FROM with_windows_payments
GROUP BY CASE WHEN running_order_id_stddev_samp_discount_2 >= 2 THEN 'LARGE' ELSE 'SMALL' END
ORDER BY order_id_stddev_samp_discount_p50 DESC NULLS LAST;
