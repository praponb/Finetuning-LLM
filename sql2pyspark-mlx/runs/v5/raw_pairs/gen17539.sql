WITH base AS (
    -- Keep recent, valid rows
    SELECT *, discount + shipping_fee AS discount_plus_shipping_fee
    FROM orders
    WHERE order_date > TO_DATE('2023-09-06')
      AND store_id IN (27, 33, 39)
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT
        s.id,
        s.customer_id,
        s.store_id,
        s.order_date,
        s.status,
        s.channel,
        s.discount,
        top1.order_item_unit_price,
        top1.order_item_quantity
    FROM base s
    LEFT JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price, c.quantity AS order_item_quantity
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
not_flagged AS (
    -- Semi-join via a correlated subquery
    SELECT s.*
    FROM latest_match s
    WHERE EXISTS (
            SELECT 1
            FROM order_items c
            WHERE c.order_id = s.id
              AND c.product_id IS NOT NULL
        )
),
deduped AS (
    -- QUALIFY-style window filter
    SELECT
        id,
        customer_id,
        status,
        channel,
        discount,
        order_item_unit_price,
        order_item_quantity
    FROM not_flagged
    QUALIFY RANK() OVER (PARTITION BY status ORDER BY order_date) = 1
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        channel,
        status,
        discount
    FROM deduped
) src
PIVOT (
    MAX(discount) AS value_sum
    FOR status IN ('RETURNED', 'CANCELLED', 'SHIPPED')
);
