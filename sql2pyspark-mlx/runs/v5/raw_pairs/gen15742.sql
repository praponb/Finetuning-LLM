WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS key, v
),
lines AS (
    SELECT
        id,
        v,
        EXPLODE(coupon_codes) AS value
    FROM flattened
),
filtered AS (
    SELECT
        *,
        CASE WHEN value IN ('SPRING10', 'WELCOME5') THEN 'Core' ELSE 'Other' END AS value_group,
        LOWER(value) AS value_lower
    FROM lines
    WHERE value IN ('BUNDLE', 'WELCOME5', 'VIP')
),
cleaned AS (
    SELECT
        id,
        v,
        value,
        value_group,
        value_lower,
        TRIM(value) AS clean_value,
        COALESCE(value, 'OTHER') AS resolved_value
    FROM filtered
    WHERE value IS NOT NULL
),
filtered_carts AS (
    SELECT *, INITCAP(TRIM(value)) AS value_title
    FROM cleaned
    WHERE value = 'SPRING10'
)
SELECT
    value,
    SIZE(COLLECT_SET(value)) AS num_values,
    COUNT(*) FILTER (WHERE value = 'WELCOME5') AS welcome5_rows,
    COUNT(CASE WHEN value = 'BUNDLE' THEN 1 END) AS bundle_count,
    COUNT_IF(value = 'BUNDLE') AS bundle_events,
    COUNT(CASE WHEN value = 'BUNDLE' THEN 1 END) AS bundle_count_2
FROM filtered_carts
GROUP BY value
ORDER BY value
