WITH latest_match AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.status,
        s.channel,
        s.amount,
        s.discount,
        s.shipping_fee,
        latest.shipped_date
    FROM orders s
    JOIN LATERAL (
            SELECT c.shipped_date
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
matched AS (
    -- Non-equi join on a validity window
    SELECT
        a.*,
        sub.id AS subscription_id,
        sub.plan,
        sub.monthly_fee
    FROM latest_match a
    LEFT JOIN subscriptions sub
        ON a.customer_id = sub.customer_id
       AND a.shipped_date >= sub.start_date
       AND a.shipped_date < sub.end_date + INTERVAL 30 DAYS
),
with_last AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT
        s.*,
        x.carrier,
        x.shipment_cost
    FROM matched s
    LEFT JOIN LATERAL (
            SELECT c.carrier, c.cost AS shipment_cost
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.delivered_date DESC, c.id DESC
            LIMIT 1
        ) x ON TRUE
),
eligible AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments x
            WHERE x.order_id = s.id
              AND x.paid_at >= s.shipped_date
        )
)
-- Join a dimension and aggregate
SELECT
    dim.country,
    src.plan,
    COUNT(*) AS cnt,
    MAX(src.shipping_fee) AS max_shipping_fee
FROM eligible src
JOIN customers dim ON src.customer_id = dim.id
GROUP BY CUBE (dim.country, src.plan);
