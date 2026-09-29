WITH parsed AS (
    -- Semi-structured parsing
    SELECT
        id,
        created_at,
        coupon_codes,
        items,
        ship_to,
        TRANSFORM(
            FILTER(items, x -> x.promo IS NOT NULL),
            x -> STRUCT(x.product_id AS pid, ROUND(x.price * 0.9, 2) AS discounted_price)
        ) AS promo_lines,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
        SIZE(items) AS num_items,
        ARRAY_JOIN(ARRAY_SORT(coupon_codes), ',') AS coupons_text,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code
    FROM carts
    WHERE SIZE(coupon_codes) > 1
)
-- Grouped metrics
SELECT
    CASE WHEN num_items >= 4 THEN 'HIGH' ELSE 'LOW' END AS num_items_tier,
    MAX(num_items) AS max_num_items,
    COUNT(DISTINCT id) AS distinct_ids,
    COUNT(DISTINCT utm_source_or_default) AS distinct_utm_source_or_defaults
FROM parsed
GROUP BY CASE WHEN num_items >= 4 THEN 'HIGH' ELSE 'LOW' END
ORDER BY distinct_ids ASC NULLS LAST;
