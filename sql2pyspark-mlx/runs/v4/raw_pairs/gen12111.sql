WITH flattened_attrs AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        ship_to,
        attributes,
        EXISTS(items, x -> x.price > 100) AS has_premium_item,
        SIZE(items) AS num_items,
        TRANSFORM(items, x -> x.product_id) AS product_ids
    FROM carts
)
SELECT
    DATE_TRUNC('HOUR', created_at) AS hour_window,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    (UNIX_TIMESTAMP(MAX(created_at)) - UNIX_TIMESTAMP(MIN(created_at))) / 60 AS span_minutes,
    MIN(created_at) AS started_at
FROM flattened_attrs
GROUP BY DATE_TRUNC('HOUR', created_at), DATE_TRUNC('DAY', created_at)
ORDER BY activity_day DESC
