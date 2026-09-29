WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) x AS k, attr_value
),
flattened_carts AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        k,
        attr_value,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM flattened
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        CONCAT_WS('-', tag, k) AS tag_k_key,
        LENGTH(tag) AS tag_length,
        COALESCE(k, 'DIRECT') AS k_clean
    FROM flattened_carts
    WHERE tag NOT IN ('WELCOME5', 'VIP')
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN k = 'device' THEN 1 ELSE 0 END AS is_device,
        TRIM(tag) AS clean_tag,
        COALESCE(k, 'OTHER') AS k_filled
    FROM cleaned
    WHERE tag_length BETWEEN 61 AND 78
)
SELECT
    id,
    attr_value,
    tag_k_key,
    tag_length,
    is_device,
    k_filled
FROM filtered
ORDER BY tag_length DESC NULLS FIRST;
