WITH not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM orders s
    WHERE EXISTS (
            SELECT 1
            FROM shipments ch
            WHERE ch.order_id = s.id
              AND ch.order_id IN (1, 2, 20, 25)
        )
),
with_latest AS (
    -- Latest matching shipment per row (LATERAL ... LIMIT 1)
    SELECT s.*, latest.carrier
    FROM not_flagged s
    LEFT JOIN LATERAL (
            SELECT c.carrier
            FROM shipments c
            WHERE c.order_id = s.id
            ORDER BY c.cost DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
kept AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM with_latest s
    WHERE NOT EXISTS (
            SELECT 1
            FROM payments c
            WHERE c.order_id = s.id
              AND c.status = 'REFUNDED'
        )
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        shipping_fee / NULLIF(discount, 0) AS shipping_fee_per_discount,
        COALESCE(carrier, 'DIRECT') AS carrier_clean
    FROM kept
    WHERE order_date >= DATE '2023-02-20'
      AND channel IS NOT NULL
)
-- Multi-level aggregation
SELECT
    CASE WHEN shipping_fee_per_discount >= 87 THEN 'LARGE' ELSE 'SMALL' END AS shipping_fee_per_discount_tier,
    SUM(CASE WHEN channel = 'partner' THEN shipping_fee ELSE 0 END) AS partner_shipping_fee,
    SUM(CASE WHEN carrier = 'UPS' THEN shipping_fee ELSE 0 END) AS ups_shipping_fee,
    COUNT(*) FILTER (WHERE channel = 'web') AS web_rows,
    SIZE(COLLECT_SET(carrier)) AS num_carriers,
    COUNT(DISTINCT id) AS distinct_ids,
    ROUND(AVG(shipping_fee_per_discount), 2) AS avg_shipping_fee_per_discount
FROM base
GROUP BY shipping_fee_per_discount_tier
