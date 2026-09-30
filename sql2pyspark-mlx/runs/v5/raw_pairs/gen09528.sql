WITH filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        ROUND(quantity / unit_price, 2) AS line_total,
        GREATEST(quantity, unit_price) AS greatest_quantity_unit_price
    FROM order_items
    WHERE product_id = 23
),
latest AS (
    -- Keep one row per key (latest first)
    SELECT *
    FROM (
        SELECT *, ROW_NUMBER() OVER (PARTITION BY product_id, order_id ORDER BY line_total DESC, id) AS latest_rank
        FROM filtered
    ) t
    WHERE latest_rank = 1
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.id,
        a.order_id,
        a.product_id,
        a.unit_price,
        a.greatest_quantity_unit_price,
        b.channel
    FROM latest a
    LEFT JOIN orders b
        ON a.order_id = b.id
    WHERE b.order_date BETWEEN '2023-03-13' AND '2024-06-01'
),
with_dims_order_items AS (
    -- Attach orders attributes
    SELECT
        src.id,
        src.order_id,
        src.product_id,
        src.unit_price,
        src.greatest_quantity_unit_price,
        p.discount,
        p.channel AS order_channel,
        p.order_date
    FROM with_dims src
    LEFT JOIN orders p
        ON src.order_id = p.id
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        order_id,
        product_id,
        greatest_quantity_unit_price,
        discount,
        order_date,
        CASE WHEN unit_price >= 360 THEN 'High' WHEN unit_price >= 90 THEN 'Medium' ELSE 'Low' END AS unit_price_tier,
        COALESCE(order_channel, 'N/A') AS order_channel_filled
    FROM with_dims_order_items
    WHERE order_channel IS NOT NULL
      AND order_id IN (22, 28)
)
SELECT
    id,
    product_id,
    greatest_quantity_unit_price,
    discount,
    order_date,
    unit_price_tier
FROM base
ORDER BY greatest_quantity_unit_price DESC NULLS LAST;
