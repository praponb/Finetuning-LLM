WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, value
),
lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        attr_key,
        value,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM flattened
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(tag) AS tag_lower,
        COALESCE(tag, 'N/A') AS tag_clean
    FROM lines
    WHERE tag IS NOT NULL
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        attr_key,
        value,
        tag,
        tag_lower,
        tag_clean,
        UPPER(tag) AS tag_upper,
        COALESCE(attr_key, 'NONE') AS attr_key_clean
    FROM filtered
    WHERE tag IS NOT NULL
)
-- Grouped metrics
SELECT
    tag,
    COUNT(*) FILTER (WHERE attr_key = 'gift') AS gift_rows,
    SIZE(COLLECT_SET(tag)) AS num_tags,
    COUNT(DISTINCT attr_key) AS distinct_attr_keys,
    COUNT(*) AS num_rows,
    COUNT(*) AS event_count
FROM base
GROUP BY tag
ORDER BY num_rows DESC NULLS LAST;
