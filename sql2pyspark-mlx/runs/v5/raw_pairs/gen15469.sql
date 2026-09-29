WITH unpacked AS (
    -- Struct fields, map lookups and JSON extraction
    SELECT
        id,
        customer_id,
        items,
        attributes,
        SIZE(attributes) AS num_attributes,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        attributes['gift'] AS gift,
        EXISTS(items, i -> i.price > 100) AS has_premium_item
    FROM carts
)
-- Multi-level aggregation
SELECT
    CASE WHEN num_attributes >= 79 THEN 'LARGE' ELSE 'SMALL' END AS num_attributes_tier,
    COUNT(*) AS row_count,
    ROUND(AVG(num_attributes), 2) AS avg_num_attributes,
    COUNT(*) AS row_count_2
FROM unpacked
GROUP BY num_attributes_tier;
