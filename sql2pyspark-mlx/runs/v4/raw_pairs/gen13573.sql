WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        created_at,
        coupon_codes,
        attributes,
        ship_to.city AS ship_city,
        SIZE(FILTER(items, item -> item.qty >= 3)) AS bulk_lines,
        ARRAY_CONTAINS(coupon_codes, 'BUNDLE') AS used_bundle,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'utm_source') AS has_utm_source,
        EXISTS(items, item -> item.price > 100) AS has_premium_item
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
)
-- Aggregate with conditional metrics
SELECT
    ship_city,
    SIZE(COLLECT_SET(ship_city)) AS num_ship_cities,
    COUNT_IF(ship_city = 'Singapore') AS singapore_events,
    MAX(bulk_lines) FILTER (WHERE ship_city = 'London') AS london_max_bulk_lines,
    COUNT(CASE WHEN ship_city = 'Berlin' THEN 1 END) AS berlin_count,
    SUM(CASE WHEN ship_city = 'Singapore' THEN bulk_lines ELSE 0 END) AS singapore_bulk_lines
FROM unpacked
GROUP BY ship_city
