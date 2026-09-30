WITH cleaned AS (
    SELECT
        id,
        order_id,
        method,
        amount,
        paid_at,
        status,
        CONCAT_WS(',', method, status) AS method_status_key
    FROM payments
    WHERE method IS NOT DISTINCT FROM 'card'
),
with_dims AS (
    SELECT
        s.*,
        o.amount AS order_amount,
        o.channel,
        o.shipping_fee
    FROM cleaned s
    LEFT JOIN orders o
        ON s.order_id = o.id
),
filtered AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        order_amount,
        channel,
        shipping_fee,
        CONCAT(channel, '/', status) AS channel_status
    FROM with_dims
    WHERE method IS NOT NULL
),
scored AS (
    SELECT
        id,
        order_id,
        paid_at,
        status,
        channel,
        shipping_fee,
        DENSE_RANK() OVER (PARTITION BY method ORDER BY order_amount ASC NULLS LAST) AS order_amount_dense_rank,
        LEAD(order_amount, 1) OVER (PARTITION BY order_id, method ORDER BY order_amount ASC, id ASC) AS next_order_amount
    FROM filtered
    WHERE method IS NOT NULL
),
metrics AS (
    SELECT
        id,
        order_id,
        paid_at,
        shipping_fee,
        order_amount_dense_rank,
        next_order_amount,
        SUM(order_amount_dense_rank) OVER w_ordered AS running_order_amount_dense_rank,
        SUM(order_amount_dense_rank) OVER (PARTITION BY channel ORDER BY order_amount_dense_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_order_amount_dense_rank_2
    FROM scored
    WINDOW w_ordered AS (PARTITION BY order_id ORDER BY order_amount_dense_rank ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
),
flagged AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC) + INTERVAL 60 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM metrics
),
sessionized AS (
    SELECT
        id,
        order_id,
        paid_at,
        shipping_fee,
        next_order_amount,
        running_order_amount_dense_rank,
        SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM flagged
)
SELECT
    CASE WHEN burst_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END AS burst_id_tier,
    CASE WHEN next_order_amount >= 1240 THEN 'PREMIUM' ELSE 'STANDARD' END AS next_order_amount_tier,
    (UNIX_TIMESTAMP(MAX(paid_at)) - UNIX_TIMESTAMP(MIN(paid_at))) / 60 AS duration_min,
    APPROX_PERCENTILE(shipping_fee, 0.95) AS shipping_fee_p95,
    MAX(running_order_amount_dense_rank) AS max_running_order_amount_dense_rank
FROM sessionized
GROUP BY GROUPING SETS ((CASE WHEN burst_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN next_order_amount >= 1240 THEN 'PREMIUM' ELSE 'STANDARD' END), (CASE WHEN burst_id >= 7 THEN 'PREMIUM' ELSE 'STANDARD' END))
ORDER BY max_running_order_amount_dense_rank DESC NULLS FIRST
