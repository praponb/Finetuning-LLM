WITH with_gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY order_id ORDER BY paid_at, id) + INTERVAL 7 DAYS THEN 1 ELSE 0 END AS session_start_flag
    FROM payments
),
sessionized AS (
    SELECT
        id,
        order_id,
        amount,
        paid_at,
        SUM(session_start_flag) OVER (PARTITION BY order_id ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS burst_id
    FROM with_gaps
)
SELECT
    s.id,
    s.order_id,
    s.amount,
    s.paid_at,
    s.burst_id,
    (SELECT SUM(s2.cost) FROM shipments s2 WHERE s2.order_id = s.order_id) AS sum_shipment_cost
FROM sessionized s
ORDER BY amount DESC NULLS LAST, paid_at DESC
