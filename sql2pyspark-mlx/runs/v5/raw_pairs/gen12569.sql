SELECT id, created_at, coupon_codes, COALESCE(attributes['utm_source'], 'direct') AS utm_source_or_default, EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code FROM carts
