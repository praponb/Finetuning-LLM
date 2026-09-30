SELECT
    id,
    created_at,
    items,
    attributes,
    COALESCE(attributes['utm_source'], 'none') AS utm_source_or_default,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'weight_kg') AS has_weight_kg,
    EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code
FROM carts;
