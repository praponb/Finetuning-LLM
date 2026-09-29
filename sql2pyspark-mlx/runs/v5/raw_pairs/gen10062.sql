WITH parsed AS (
    SELECT
        id,
        customer_id,
        created_at,
        ship_to,
        ARRAY_CONTAINS(MAP_KEYS(attributes), 'device') AS has_device,
        ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS used_welcome5,
        COALESCE(attributes['gift'], 'unknown') AS gift_or_default,
        SIZE(FILTER(items, x -> x.qty >= 3)) AS bulk_lines
    FROM carts
)
SELECT
    CASE WHEN bulk_lines >= 3 THEN 'LARGE' ELSE 'SMALL' END AS bulk_lines_tier,
    COUNT(DISTINCT customer_id) AS distinct_customer_ids,
    ROUND(AVG(bulk_lines), 2) AS avg_bulk_lines
FROM parsed
GROUP BY bulk_lines_tier;
