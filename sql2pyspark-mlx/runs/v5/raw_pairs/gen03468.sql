SELECT
    id,
    customer_id,
    coupon_codes,
    attributes,
    COALESCE(attributes['device'], 'direct') AS device_or_default,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'weight_kg') AS has_weight_kg,
    TRANSFORM(items, item -> item.product_id) AS product_ids,
    COALESCE(attributes['utm_source'], 'direct') AS utm_source_or_default,
    SIZE(attributes) AS num_attributes
FROM carts;
