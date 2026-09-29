SELECT
    id,
    customer_id,
    attributes,
    ARRAY_CONTAINS(coupon_codes, 'BUNDLE') AS used_bundle,
    SIZE(items) AS num_items
FROM carts
WHERE customer_id = 8;
