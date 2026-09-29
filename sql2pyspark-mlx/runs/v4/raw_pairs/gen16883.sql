WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) li AS attr_key, v
),
flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        attr_key,
        v,
        code
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) li AS code
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, LOWER(code) AS code_lower
    FROM flattened
    WHERE attr_key IN ('utm_source', 'gift', 'device')
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        v,
        code,
        code_lower,
        CONCAT_WS('-', attr_key, code) AS attr_key_code_key
    FROM valid
    WHERE code IS NOT NULL
      AND attr_key NOT IN ('weight_kg', 'gift', 'device')
),
cleaned_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        TRIM(code) AS clean_code,
        CASE WHEN code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle
    FROM cleaned
    WHERE code IS NOT NULL
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(is_bundle * 1.07, 2) AS is_bundle_adjusted,
        LENGTH(code) AS code_length,
        COALESCE(code, 'OTHER') AS code_clean
    FROM cleaned_carts
    WHERE code IS NOT NULL
)
-- Grouped metrics
SELECT
    code,
    AVG(code_length) FILTER (WHERE code = 'BUNDLE') AS bundle_avg_code_length,
    SUM(CASE WHEN code = 'WELCOME5' THEN is_bundle_adjusted ELSE 0 END) AS welcome5_is_bundle_adjusted,
    SUM(CASE WHEN code = 'SPRING10' THEN is_bundle_adjusted ELSE 0 END) AS spring10_is_bundle_adjusted
FROM base
GROUP BY 1
HAVING bundle_avg_code_length < 0
ORDER BY spring10_is_bundle_adjusted;
