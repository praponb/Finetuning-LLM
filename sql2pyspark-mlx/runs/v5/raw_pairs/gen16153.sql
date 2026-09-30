SELECT
    id,
    coupon_codes,
    items,
    ship_to,
    attributes,
    COALESCE(attributes['device'], 'unknown') AS device_or_default,
    EXISTS(items, x -> x.price > 100) AS has_premium_item
FROM carts
