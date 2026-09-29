SELECT
    id,
    customer_id,
    items,
    ship_to,
    ARRAY_CONTAINS(coupon_codes, 'VIP') AS used_vip,
    SIZE(attributes) AS num_attributes,
    ship_to.zip AS ship_zip,
    ship_to.city AS ship_city,
    attributes['gift'] AS gift
FROM carts;
