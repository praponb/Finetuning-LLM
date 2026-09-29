WITH ranked AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        order_id,
        amount,
        status
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY method ORDER BY amount DESC NULLS LAST, id) AS row_num
        FROM payments
    ) t
    WHERE row_num = 1
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN status = 'REFUNDED' THEN 1 ELSE 0 END is_refunded
    FROM ranked
    WHERE order_id IS NOT NULL
      AND status = 'FAILED'
),
cleaned_payments AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        amount,
        status,
        is_refunded,
        CASE WHEN amount >= 1300 THEN 'High' WHEN amount >= 300 THEN 'Medium' ELSE 'Low' END amount_tier
    FROM cleaned
    WHERE status = 'FAILED'
),
enriched AS (
    -- Ranking and gap features
    SELECT
        id,
        amount,
        is_refunded,
        amount_tier,
        SUM(is_refunded) OVER (PARTITION BY order_id) order_id_sum_is_refunded,
        LAG(amount, 1, 0) OVER w_part prev_amount,
        LEAD(is_refunded, 1) OVER w_recent next_is_refunded
    FROM cleaned_payments
    WINDOW w_part AS (PARTITION BY status, order_id ORDER BY amount, id), w_recent AS (ORDER BY amount, id)
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        id,
        amount,
        is_refunded,
        amount_tier,
        order_id_sum_is_refunded,
        PERCENT_RANK() OVER w_part prev_amount_percent_rank,
        LEAD(next_is_refunded, 1, 0) OVER (ORDER BY order_id_sum_is_refunded, id) next_next_is_refunded
    FROM enriched
    WINDOW w_part AS (ORDER BY prev_amount NULLS LAST)
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN is_refunded >= 79 THEN 'LARGE' ELSE 'SMALL' END AS is_refunded_tier,
        CASE WHEN prev_amount_percent_rank >= 0.77 THEN 'LARGE' ELSE 'SMALL' END AS prev_amount_percent_rank_tier,
        ROUND(AVG(is_refunded), 2) AS avg_is_refunded,
        APPROX_PERCENTILE(amount, 0.5) AS amount_p50,
        COUNT(*) AS row_count,
        APPROX_PERCENTILE(amount, 0.95) AS amount_p95,
        AVG(order_id_sum_is_refunded) AS avg_order_id_sum_is_refunded,
        GROUPING_ID() AS grouping_level
    FROM with_windows
    GROUP BY CUBE (CASE WHEN is_refunded >= 79 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN prev_amount_percent_rank >= 0.77 THEN 'LARGE' ELSE 'SMALL' END)
    HAVING row_count > 1
)
SELECT
    is_refunded_tier,
    prev_amount_percent_rank_tier,
    avg_is_refunded,
    amount_p50,
    row_count,
    amount_p95,
    avg_order_id_sum_is_refunded
FROM summary
ORDER BY avg_is_refunded DESC;
