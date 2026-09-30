WITH scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(amount) OVER (PARTITION BY status, order_id ORDER BY amount, id ROWS BETWEEN 3 PRECEDING AND 1 PRECEDING) AS amount_avg_3_prior,
        AVG(amount) OVER (PARTITION BY order_id ORDER BY CAST(paid_at AS BIGINT) RANGE BETWEEN 7776000 PRECEDING AND CURRENT ROW) AS amount_avg_90d
    FROM payments
)
SELECT
    s.id,
    s.order_id,
    s.amount,
    s.paid_at,
    s.status,
    s.amount_avg_3_prior,
    s.amount_avg_90d,
    (SELECT SUM(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS sum_shipment_cost
FROM scored s
