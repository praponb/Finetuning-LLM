WITH filtered AS (
    SELECT
        *,
        ROUND(amount, 1) AS amount_rounded,
        COALESCE(amount, 0) AS amount_filled
    FROM payments
    WHERE paid_at >= DATE '2023-06-07'
      AND method = 'cash'
),
with_gaps AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY method ORDER BY paid_at, id) > INTERVAL '14' DAY THEN 1 ELSE 0 END AS is_new_session
    FROM filtered
),
sessions_payments AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY method ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
),
with_dims AS (
    SELECT
        src.*,
        d.channel,
        d.discount
    FROM sessions_payments src
    INNER JOIN orders d
        ON src.order_id = d.id
),
metrics AS (
    SELECT
        id,
        order_id,
        method,
        paid_at,
        status,
        is_new_session,
        burst_id,
        channel,
        MIN(discount) OVER (PARTITION BY channel ORDER BY amount_filled, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_discount,
        AVG(discount) OVER (PARTITION BY channel ORDER BY amount_filled, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_discount,
        MAX(discount) OVER (PARTITION BY channel ORDER BY amount_filled, id ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_max_discount
    FROM with_dims
),
ranked AS (
    SELECT
        id,
        method,
        paid_at,
        burst_id,
        channel,
        last7_min_discount,
        last7_avg_discount,
        last7_max_discount
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY channel ORDER BY paid_at NULLS LAST, id) AS dedup_rank
        FROM metrics
    ) ranked
    WHERE dedup_rank = 1
),
flagged AS (
    SELECT *, CASE WHEN paid_at - LAG(paid_at) OVER (PARTITION BY channel ORDER BY paid_at, id) > INTERVAL '90' DAY THEN 1 ELSE 0 END AS is_new_burst
    FROM ranked
),
sessionized AS (
    SELECT *, SUM(is_new_burst) OVER (PARTITION BY channel ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id_2
    FROM flagged
)
SELECT
    id,
    method,
    burst_id,
    channel,
    last7_min_discount,
    last7_avg_discount,
    last7_max_discount,
    CASE WHEN channel RLIKE '^(store|mobile)$' THEN 'CORE' WHEN channel RLIKE '^(web|partner)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS channel_zone
FROM sessionized
