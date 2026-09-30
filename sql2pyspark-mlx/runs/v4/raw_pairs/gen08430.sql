WITH scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROW_NUMBER() OVER (PARTITION BY method ORDER BY paid_at, id) AS row_seq,
        LAST_VALUE(amount) IGNORE NULLS OVER (PARTITION BY method ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_amount,
        COUNT(amount) OVER (PARTITION BY method) AS method_count_amount
    FROM payments
)
SELECT
    s.id,
    s.order_id,
    s.amount,
    s.paid_at,
    s.status,
    s.last_known_amount,
    s.method_count_amount,
    (SELECT AVG(x.cost) FROM shipments x WHERE x.order_id = s.order_id) AS avg_shipment_cost
FROM scored s
ORDER BY method_count_amount NULLS FIRST
