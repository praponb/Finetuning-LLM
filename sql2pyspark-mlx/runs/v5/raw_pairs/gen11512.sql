WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, v
),
kept_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM lines
    INTERSECT ALL
    SELECT id
    FROM carts
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
reconciled AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN kept_keys k ON s.id = k.id
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SIZE(coupon_codes) AS coupon_codes_count
    FROM reconciled
    WHERE k IS DISTINCT FROM 'gift'
      AND ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
base_carts AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        coupon_codes,
        k,
        v,
        coupon_codes_count,
        CAST(coupon_codes_count AS STRING) AS coupon_codes_count_str,
        ROUND(coupon_codes_count, 0) AS coupon_codes_count_rounded
    FROM base
    WHERE ARRAY_CONTAINS(coupon_codes, 'WELCOME5')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(coupon_codes_count_rounded, 0) AS coupon_codes_count_rounded_rounded,
        CASE WHEN k IN ('gift', 'weight_kg') THEN 'Tier 1' ELSE 'Other' END AS k_group
    FROM base_carts
    WHERE k IS NOT NULL
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    k,
    metric,
    val
FROM (
    SELECT
        k,
        coupon_codes_count_rounded_rounded,
        coupon_codes_count_rounded
    FROM valid
) src
UNPIVOT (
    val FOR metric IN (coupon_codes_count_rounded_rounded AS COUPON_CODES_COUNT_ROUNDED_ROUNDED_VAL, coupon_codes_count_rounded AS COUPON_CODES_COUNT_ROUNDED)
);
