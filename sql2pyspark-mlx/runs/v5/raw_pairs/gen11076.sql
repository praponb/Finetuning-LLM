WITH exploded AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS attr_key, attr_value
),
exploded_carts AS (
    SELECT
        id,
        created_at,
        attr_key,
        element
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) li AS element
),
valid AS (
    SELECT
        *,
        DATE_FORMAT(created_at, 'MMM yyyy') AS created_at_label,
        DATE_TRUNC('YEAR', created_at) AS created_at_period,
        COALESCE(attr_key, 'UNKNOWN') AS attr_key_clean
    FROM exploded_carts
    WHERE created_at > TO_DATE('2025-03-02')
),
valid_carts AS (
    SELECT
        id,
        created_at,
        attr_key,
        element,
        created_at_label,
        attr_key_clean,
        LENGTH(element) AS element_length
    FROM valid
    WHERE created_at >= TO_DATE('2025-03-01')
)
SELECT
    CASE WHEN element_length >= 68 THEN 'HIGH' ELSE 'LOW' END AS element_length_tier,
    ROUND(SUM(element_length) / NULLIF(COUNT(CASE WHEN attr_key = 'device' THEN 1 END), 0), 2) AS element_length_per_device,
    MAX(created_at) AS last_created_at,
    MAX(created_at) AS last_created_at_2
FROM valid_carts
GROUP BY CASE WHEN element_length >= 68 THEN 'HIGH' ELSE 'LOW' END
ORDER BY element_length_tier;
