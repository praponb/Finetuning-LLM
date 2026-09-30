WITH unpacked AS (
    SELECT
        id,
        created_at,
        ship_to,
        COALESCE(attributes['utm_source'], 'direct') AS utm_source_or_default,
        EXISTS(coupon_codes, c -> c LIKE 'W%') AS has_welcome_code,
        SIZE(items) AS num_items,
        attributes['gift'] AS gift,
        ARRAY_CONTAINS(coupon_codes, 'WELCOME5') AS used_welcome5
    FROM carts
)
SELECT
    id,
    num_items,
    gift,
    SUM(num_items) OVER (ORDER BY created_at ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_sum_num_items,
    AVG(num_items) OVER (ORDER BY created_at ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_avg_num_items,
    MIN(num_items) OVER (ORDER BY created_at ASC, id ASC ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS last7_min_num_items
FROM unpacked
