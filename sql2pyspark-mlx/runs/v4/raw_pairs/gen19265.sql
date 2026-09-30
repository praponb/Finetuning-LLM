WITH gaps AS (
    SELECT *, CASE WHEN LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) IS NULL
                 OR paid_at > LAG(paid_at) OVER (PARTITION BY status ORDER BY paid_at, id) + INTERVAL 90 DAYS THEN 1 ELSE 0 END AS is_new_session
    FROM payments
),
sessions_payments AS (
    SELECT *, SUM(is_new_session) OVER (PARTITION BY status ORDER BY paid_at, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS session_no
    FROM gaps
)
SELECT
    s.id,
    s.order_id,
    s.method,
    s.paid_at,
    s.status,
    s.is_new_session,
    CASE WHEN method RLIKE '^(promptpay|paypal)$' THEN 'GROUP_B' ELSE 'OTHER_ZONE' END AS method_zone,
    (SELECT SUM(t2.cost) FROM shipments t2 WHERE t2.order_id = s.order_id) AS sum_shipment_cost
FROM sessions_payments s;
