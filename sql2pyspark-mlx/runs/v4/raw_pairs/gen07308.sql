WITH lines AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, attr_value
),
flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        attr_value,
        EXPLODE(coupon_codes) AS code
    FROM lines
),
cleaned AS (
    SELECT
        *,
        HOUR(created_at) AS created_at_hour,
        CASE WHEN code IN ('BUNDLE', 'VIP') THEN 'Group A' ELSE 'Other' END AS code_group
    FROM flattened
    WHERE created_at > TIMESTAMP '2025-03-02 06:00:00'
),
enriched AS (
    SELECT
        src.*,
        c.city,
        c.segment
    FROM cleaned src
    JOIN customers c
        ON src.customer_id = c.id
),
summary AS (
    SELECT
        DATE_TRUNC('DAY', created_at) AS activity_day,
        code,
        CASE WHEN created_at_hour >= 63 THEN 'LARGE' ELSE 'SMALL' END AS created_at_hour_tier,
        COUNT(CASE WHEN segment = 'Corporate' THEN 1 END) AS corporate_count,
        COUNT(DISTINCT city) AS distinct_cities
    FROM enriched
    GROUP BY activity_day, code, created_at_hour_tier
)
SELECT
    activity_day,
    code,
    created_at_hour_tier,
    corporate_count
FROM summary
ORDER BY code, created_at_hour_tier DESC;
