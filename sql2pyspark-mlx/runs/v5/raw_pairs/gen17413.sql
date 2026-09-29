SELECT
    id,
    customer_id,
    created_at,
    coupon_codes,
    attributes,
    SIZE(attributes) AS num_attributes,
    EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
    COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
    ship_to.zip AS ship_zip
FROM carts
