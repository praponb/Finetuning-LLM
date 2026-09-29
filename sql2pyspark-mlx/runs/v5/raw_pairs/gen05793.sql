SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    items,
    AGGREGATE(items, 0D, (acc, item) -> acc + item.qty * item.price, acc -> ROUND(acc, 2)) AS items_total,
    TRY_CAST(attributes['weight_kg'] AS DOUBLE) AS weight_kg,
    COALESCE(attributes['device'], 'direct') AS device_or_default,
    attributes['utm_source'] AS utm_source,
    TRANSFORM(items, item -> item.product_id) AS product_ids
FROM carts;
