WITH unpacked AS (
    -- Semi-structured parsing
    SELECT
        id,
        customer_id,
        ship_to,
        COALESCE(attributes['gift'], 'unknown') AS gift_or_default,
        ARRAY_MAX(TRANSFORM(items, item -> item.price)) AS max_item_price
    FROM carts
    WHERE customer_id IN (8, 24)
)
-- Running and moving aggregates
SELECT
    id,
    gift_or_default,
    SUM(max_item_price) OVER (PARTITION BY customer_id ORDER BY max_item_price, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_max_item_price,
    ROUND(max_item_price / SUM(max_item_price) OVER (PARTITION BY customer_id), 4) AS max_item_price_share
FROM unpacked
