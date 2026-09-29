WITH valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(shipping_fee, 0) AS shipping_fee_filled,
        discount / NULLIF(shipping_fee, 0) AS discount_per_shipping_fee
    FROM orders
    WHERE order_date >= DATE '2023-03-21'
),
not_flagged AS (
    -- Anti-join via a correlated subquery
    SELECT s.*
    FROM valid s
    WHERE NOT EXISTS (
            SELECT 1
            FROM order_items ch
            WHERE ch.order_id = s.id
              AND ch.product_id IN (5, 20)
        )
),
with_last AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.order_date,
        s.status,
        s.channel,
        s.amount,
        s.discount,
        s.shipping_fee,
        s.shipping_fee_filled,
        latest.order_item_quantity
    FROM not_flagged s
    JOIN LATERAL (
            SELECT c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) latest ON TRUE
),
eligible AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM with_last s
    WHERE EXISTS (
            SELECT 1
            FROM shipments x
            WHERE x.order_id = s.id
              AND x.delivered_date < s.order_date
        )
),
ranked AS (
    -- QUALIFY-style window filter
    SELECT *
    FROM eligible
    QUALIFY ROW_NUMBER() OVER (PARTITION BY channel ORDER BY discount NULLS LAST, id) = 1
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        LAG(order_item_quantity, 1) OVER (PARTITION BY status ORDER BY amount, id) AS prev_order_item_quantity,
        MAX(order_item_quantity) OVER (PARTITION BY channel) AS channel_max_order_item_quantity
    FROM ranked
    WHERE order_item_quantity > 5
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        channel,
        discount,
        shipping_fee_filled,
        shipping_fee,
        amount
    FROM scored
) src
UNPIVOT (
    value FOR metric_name IN (discount AS DISCOUNT, shipping_fee_filled AS SHIPPING_FEE_FILLED, shipping_fee AS SHIPPING_FEE, amount AS AMOUNT)
);
