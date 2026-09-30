WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL '30' DAY THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(session_start_flag) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS island_id
    FROM with_gaps
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.amount,
    s.paid_at,
    s.status,
    s.session_start_flag,
    s.island_id,
    CASE WHEN status RLIKE '^(SUCCESS|REFUNDED)$' THEN 'EXTENDED' WHEN status RLIKE '^(FAILED)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS status_zone,
    (SELECT AVG(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS avg_shipment_cost
FROM sessions_payments s
ORDER BY id DESC NULLS FIRST;
