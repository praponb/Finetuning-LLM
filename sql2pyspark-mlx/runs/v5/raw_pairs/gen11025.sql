WITH valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        order_id,
        product_id,
        ROUND(unit_price, 0) unit_price_rounded,
        ROUND(quantity, 2) quantity_rounded
    FROM order_items
    WHERE product_id IS NOT NULL
),
with_dims AS (
    -- Attach orders attributes
    SELECT
        a.*,
        o.channel,
        o.discount
    FROM valid a
    LEFT JOIN orders o
        ON a.order_id = o.id
    WHERE o.channel != 'web'
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        LENGTH(channel) channel_length,
        COALESCE(channel, 'DIRECT') resolved_channel
    FROM with_dims
    WHERE channel IS NOT NULL
      AND unit_price_rounded > 62
),
with_dims_order_items AS (
    -- Attach orders attributes
    SELECT a.*, o.amount
    FROM filtered a
    INNER JOIN orders o
        ON a.order_id = o.id
),
enriched AS (
    -- Attach products attributes
    SELECT
        e.id,
        e.product_id,
        e.unit_price_rounded,
        e.quantity_rounded,
        e.discount,
        e.channel_length,
        e.resolved_channel,
        e.amount,
        p.cost,
        p.price,
        p.product_name
    FROM with_dims_order_items e
    LEFT JOIN products p
        ON e.product_id <=> p.id
)
SELECT
    id,
    product_id,
    quantity_rounded,
    discount,
    resolved_channel,
    cost,
    price
FROM enriched
ORDER BY product_id DESC;
