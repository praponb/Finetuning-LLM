WITH parsed AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        attributes,
        SIZE(FILTER(items, li -> li.qty >= 3)) AS bulk_lines,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        SIZE(attributes) AS num_attributes,
        COALESCE(attributes['utm_source'], 'unknown') AS utm_source_or_default,
        ship_to.city AS ship_city
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'VIP')
)
-- Running and moving aggregates
SELECT
    id,
    created_at,
    bulk_lines,
    has_welcome_code,
    num_attributes,
    ship_city,
    MIN(bulk_lines) OVER (PARTITION BY customer_id, ship_city ORDER BY bulk_lines, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_min_bulk_lines,
    MAX(bulk_lines) OVER (PARTITION BY customer_id, ship_city ORDER BY bulk_lines, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_max_bulk_lines,
    AVG(bulk_lines) OVER (PARTITION BY customer_id, ship_city ORDER BY bulk_lines, id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) running_avg_bulk_lines
FROM parsed;
