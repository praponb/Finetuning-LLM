WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, value
),
exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        EXPLODE(coupon_codes) AS tag
    FROM lines
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, UPPER(tag) AS tag_upper
    FROM exploded
    WHERE tag IS NOT NULL
      AND tag <> 'VIP'
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(tag) AS clean_tag
    FROM filtered
    WHERE tag = 'VIP'
),
totals AS (
    -- Multi-level aggregation
    SELECT
        tag,
        customer_id,
        COUNT(CASE WHEN tag = 'VIP' THEN 1 END) AS vip_count,
        COUNT(*) AS cnt
    FROM valid
    GROUP BY CUBE (tag, customer_id)
)
SELECT customer_id, cnt
FROM totals
ORDER BY cnt DESC, customer_id;
