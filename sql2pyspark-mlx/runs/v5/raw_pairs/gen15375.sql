SELECT
    id,
    customer_id,
    coupon_codes,
    ship_to,
    COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
    ARRAY_CONTAINS(MAP_KEYS(attributes), 'utm_source') AS has_utm_source,
    ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
    attributes['gift'] AS gift
FROM carts
WHERE customer_id IN (13, 37)
