WITH base AS (
    SELECT
        id,
        order_id,
        paid_at,
        status,
        CEIL(amount) AS amount_whole,
        CASE WHEN status = 'FAILED' THEN 1 ELSE 0 END AS is_failed,
        COALESCE(method, 'DIRECT') AS resolved_method
    FROM payments
    WHERE paid_at > TIMESTAMP '2024-04-18 05:00:00'
      AND method IS NOT NULL
),
enriched AS (
    SELECT
        e.*,
        b.channel,
        b.order_date
    FROM base e
    LEFT JOIN orders b
        ON e.order_id = b.id
),
joined AS (
    SELECT a.*, p.channel AS order_channel
    FROM enriched a
    LEFT JOIN orders p
        ON a.order_id = p.id
),
prev_events AS (
    SELECT *, LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC) AS prev_paid_at
    FROM joined
),
with_gaps AS (
    SELECT *, CASE WHEN prev_paid_at IS NULL OR paid_at > prev_paid_at + INTERVAL 14 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM prev_events
),
islands AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at ASC, id ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    s.id,
    s.order_id,
    s.paid_at,
    s.status,
    s.amount_whole,
    s.is_failed,
    s.resolved_method,
    s.order_channel,
    s.prev_paid_at,
    (SELECT MAX(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS max_shipment_cost
FROM islands s
ORDER BY prev_paid_at DESC
