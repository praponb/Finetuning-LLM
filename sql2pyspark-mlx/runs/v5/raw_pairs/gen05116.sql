WITH gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM payments
),
islands AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_id
    FROM gaps
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id, status ORDER BY amount NULLS LAST, id) AS rank_in_group
    FROM islands
    QUALIFY rank_in_group = 1
),
prev_events AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) AS prev_paid_at
    FROM ranked
),
with_gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM prev_events
),
sessionized AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        SUM(session_start_flag) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
),
enriched AS (
    SELECT
        *,
        MAX(amount) OVER (PARTITION BY order_id) AS order_id_max_amount,
        STDDEV_SAMP(island_id) OVER (PARTITION BY method) AS method_stddev_samp_island_id
    FROM sessionized
),
summary AS (
    SELECT
        order_id,
        method,
        (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS span_minutes,
        COUNT(DISTINCT order_id) AS distinct_order_ids,
        GROUPING(order_id) AS grouping_id
    FROM enriched
    GROUP BY ROLLUP (order_id, method)
)
SELECT method, CASE WHEN method RLIKE '^(card|bank_transfer)$' THEN 'PRIORITY' WHEN method RLIKE '^(paypal|cash)$' THEN 'OTHER_ZONE' ELSE 'GROUP_B' END AS method_zone
FROM summary
ORDER BY method;
