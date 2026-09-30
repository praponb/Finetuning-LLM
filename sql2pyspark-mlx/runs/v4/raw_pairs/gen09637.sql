WITH base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        customer_id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        ROUND(shipping_fee * 1.1, 2) AS adjusted_shipping_fee,
        INITCAP(TRIM(channel)) AS channel_title
    FROM orders
    WHERE status IN ('SHIPPED', 'RETURNED', 'COMPLETED')
),
latest_match AS (
    -- Latest matching order_item per row (LATERAL ... LIMIT 1)
    SELECT s.*, top1.order_item_unit_price
    FROM base s
    JOIN LATERAL (
            SELECT c.unit_price AS order_item_unit_price
            FROM order_items c
            WHERE c.order_id = s.id
            ORDER BY c.unit_price DESC, c.id DESC
            LIMIT 1
        ) top1 ON TRUE
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        store_id,
        order_date,
        status,
        channel,
        amount,
        adjusted_shipping_fee,
        channel_title,
        order_item_unit_price,
        CEIL(adjusted_shipping_fee) AS adjusted_shipping_fee_whole,
        COALESCE(channel, 'NONE') AS channel_clean
    FROM latest_match
    WHERE adjusted_shipping_fee <= 63
)
-- Native SQL UNPIVOT (wide to long)
SELECT *
FROM (
    SELECT
        id,
        store_id,
        adjusted_shipping_fee,
        amount,
        order_item_unit_price
    FROM filtered
) src
UNPIVOT (
    metric_value FOR metric IN (adjusted_shipping_fee AS ADJUSTED_SHIPPING_FEE, amount, order_item_unit_price AS ORDER_ITEM_UNIT_PRICE_VAL)
)
