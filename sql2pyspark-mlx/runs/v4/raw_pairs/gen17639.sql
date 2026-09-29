SELECT
    id,
    coupon_codes,
    items,
    COALESCE(attributes['device'], 'direct') AS device_or_default,
    ship_to.city AS ship_city,
    attributes['gift'] AS gift,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'gift') AS has_gift,
    EXISTS(items, i -> i.price > 100) AS has_premium_item
FROM carts
