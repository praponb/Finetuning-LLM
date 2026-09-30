SELECT
    id,
    created_at,
    coupon_codes,
    items,
    ship_to,
    attributes,
    ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS used_spring10,
    COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'device') AS has_device
FROM carts
WHERE SIZE(coupon_codes) >= 2
