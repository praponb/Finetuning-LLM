WITH lines AS (
    SELECT
        id,
        coupon_codes,
        key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS key, attr_value
),
valid AS (
    SELECT
        *,
        SUBSTRING(key, 1, 3) key_prefix,
        COALESCE(key, 'UNKNOWN') key_clean
    FROM lines
    WHERE key IS NOT NULL
),
base AS (
    SELECT
        *,
        LENGTH(key) key_length,
        COALESCE(key, 'DIRECT') key_clean_2
    FROM valid
    WHERE key IS DISTINCT FROM 'weight_kg'
      AND SIZE(coupon_codes) > 1
),
valid_carts AS (
    SELECT *, CASE WHEN key_length >= 63 THEN 'High' WHEN key_length >= 30 THEN 'Medium' ELSE 'Low' END key_length_level
    FROM base
    WHERE key IS NOT NULL
      AND key IS DISTINCT FROM 'utm_source'
)
SELECT
    id,
    coupon_codes,
    key,
    attr_value,
    key_prefix
FROM valid_carts
ORDER BY key DESC, id DESC;
