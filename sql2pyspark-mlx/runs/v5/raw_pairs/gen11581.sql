WITH base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        discount,
        shipping_fee,
        CAST(amount AS INT) AS amount_int
    FROM orders
    WHERE order_date >= TO_DATE('2023-08-19')
      AND customer_id IS NOT NULL
),
latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, x.shipment_status
    FROM base s
    JOIN LATERAL (
            SELECT c.status AS shipment_status
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
with_latest AS (
    -- Latest matching payment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.store_id,
        s.status,
        s.channel,
        s.amount,
        s.discount,
        s.shipment_status,
        latest.payment_status
    FROM latest_match s
    JOIN LATERAL (
            SELECT c.status AS payment_status
            FROM payments c
            WHERE c.order_id = s.id
            ORDER BY c.paid_at DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.cost IS NOT NULL
        )
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        FIRST_VALUE(amount, TRUE) OVER (ORDER BY amount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_amount,
        FIRST_VALUE(shipment_status, TRUE) OVER (ORDER BY discount, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_shipment_status
    FROM not_flagged
)
-- Join a dimension and aggregate
SELECT
    s.region,
    s.city,
    a.status,
    COUNT(*) AS num_rows,
    MIN(a.discount) AS min_discount,
    MAX(a.first_known_amount) AS max_first_known_amount
FROM enriched a
LEFT JOIN stores s ON a.store_id = s.id
GROUP BY GROUPING SETS ((s.region, s.city, a.status), (s.region, s.city), (s.region), ())
ORDER BY num_rows NULLS FIRST
