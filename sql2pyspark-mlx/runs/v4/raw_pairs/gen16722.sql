WITH cleaned AS (
    SELECT *, COALESCE(carrier, 'N/A') AS carrier_filled
    FROM shipments
    WHERE delivered_date > TO_DATE('2023-11-27')
      AND order_id IS NOT NULL
      AND cost <= 37
),
with_dims AS (
    SELECT
        src.*,
        d.status AS order_status,
        d.shipping_fee,
        d.channel
    FROM cleaned src
    INNER JOIN orders d
        ON src.order_id = d.id
),
latest AS (
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee NULLS FIRST, id) AS dedup_rank
        FROM with_dims
    ) ranked
    WHERE dedup_rank = 1
),
ranked AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY status ORDER BY shipping_fee DESC NULLS LAST, id) AS rn
    FROM latest
    QUALIFY rn = 1
),
latest_shipments AS (
    SELECT *, ROW_NUMBER() OVER (PARTITION BY order_id ORDER BY shipped_date DESC, id) AS rn_2
    FROM ranked
    QUALIFY rn_2 = 1
)
SELECT *
FROM (
    SELECT
        id,
        order_status,
        cost,
        shipping_fee
    FROM latest_shipments
) src
UNPIVOT (
    value FOR kpi IN (cost AS COST, shipping_fee AS SHIPPING_FEE_VAL)
);
