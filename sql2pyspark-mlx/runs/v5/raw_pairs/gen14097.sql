SELECT
    id,
    customer_id,
    created_at,
    COALESCE(attributes['device'], 'unknown') AS device_or_default,
    attributes['device'] AS device,
    ARRAY_CONTAINS(coupon_codes, 'VIP') AS used_vip,
    attributes['gift'] AS gift
FROM carts;
