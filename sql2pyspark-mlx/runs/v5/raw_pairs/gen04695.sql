WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, v
),
exploded AS (
    SELECT
        created_at,
        v,
        value
    FROM flattened
    LATERAL VIEW EXPLODE(coupon_codes) li AS value
),
filtered AS (
    SELECT
        *,
        QUARTER(created_at) AS created_at_quarter,
        HOUR(created_at) AS created_at_hour
    FROM exploded
    WHERE HOUR(created_at) BETWEEN 9 AND 14
),
enriched AS (
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY value ORDER BY created_at_hour DESC NULLS LAST) AS created_at_hour_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY value ORDER BY created_at_hour DESC NULLS LAST) AS created_at_hour_percent_rank
    FROM filtered
)
SELECT
    created_at,
    v,
    value,
    created_at_quarter,
    created_at_hour,
    created_at_hour_dense_rank,
    CASE WHEN value RLIKE '^(VIP|WELCOME5)$' THEN 'PRIORITY' WHEN value RLIKE '^(SPRING10|BUNDLE)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS value_zone
FROM enriched
ORDER BY created_at_hour_dense_rank DESC
