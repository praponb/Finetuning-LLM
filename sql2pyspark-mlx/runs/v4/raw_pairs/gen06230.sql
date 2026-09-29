SELECT
    id,
    coupon_codes,
    items,
    attributes,
    ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS used_welcome5,
    COALESCE(ship_to.zip, 'UNKNOWN') AS zip_or_unknown,
    attributes['device'] AS device,
    ARRAY_MAX(TRANSFORM(items, x -> x.price)) AS max_item_price,
    attributes['utm_source'] AS utm_source
FROM carts;
