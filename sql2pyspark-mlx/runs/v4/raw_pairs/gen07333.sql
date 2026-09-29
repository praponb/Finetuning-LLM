WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS element
    FROM carts
)
-- Multi-level aggregation
SELECT
    customer_id,
    DATE_TRUNC('DAY', created_at) AS activity_day,
    COUNT_IF(element = 'BUNDLE') AS bundle_events,
    SIZE(COLLECT_SET(element)) AS num_elements
FROM lines
GROUP BY 1, 2
HAVING SIZE(COLLECT_SET(element)) >= 1
ORDER BY bundle_events DESC NULLS FIRST
