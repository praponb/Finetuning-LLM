SELECT
    id,
    customer_id,
    coupon_codes,
    items,
    ship_to,
    attributes,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'gift') AS has_gift,
    SIZE(items) AS num_items,
    attributes['device'] AS device
FROM carts;
