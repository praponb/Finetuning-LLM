SELECT
    id,
    coupon_codes,
    attributes,
    attributes['device'] AS device,
    attributes['utm_source'] AS utm_source
FROM carts
WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5');
