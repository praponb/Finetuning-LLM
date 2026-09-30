WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) t AS attr_key, v
),
valid AS (
    SELECT
        id,
        coupon_codes,
        attr_key,
        v,
        ARRAY_CONTAINS(coupon_codes, 'BUNDLE') is_bundle,
        INITCAP(TRIM(attr_key)) attr_key_title,
        COALESCE(attr_key, 'NONE') resolved_attr_key
    FROM lines
    WHERE created_at >= DATE '2025-03-01'
),
base AS (
    SELECT *, TRIM(attr_key) clean_attr_key
    FROM valid
    WHERE attr_key IS NOT NULL
),
filtered AS (
    SELECT
        *,
        SUBSTRING(attr_key, 1, 4) attr_key_prefix,
        COALESCE(attr_key, 'NONE') resolved_attr_key_2
    FROM base
    WHERE NOT is_bundle
)
SELECT
    attr_key,
    COUNT(DISTINCT attr_key) AS distinct_attr_keys,
    COUNT(DISTINCT id) AS distinct_ids
FROM filtered
GROUP BY attr_key
