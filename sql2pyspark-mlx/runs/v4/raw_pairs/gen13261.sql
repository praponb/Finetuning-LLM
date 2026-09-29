WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, attr_value
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        SIZE(coupon_codes) AS coupon_codes_count,
        SUBSTRING(k, 1, 2) AS k_prefix,
        COALESCE(k, 'OTHER') AS k_clean
    FROM lines
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        coupon_codes,
        k_prefix,
        CAST(coupon_codes_count AS DOUBLE) AS coupon_codes_count_num
    FROM filtered
    WHERE coupon_codes_count BETWEEN 22 AND 43
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        COALESCE(coupon_codes_count_num, 0) AS coupon_codes_count_num_filled,
        ARRAY_CONTAINS(coupon_codes, 'SPRING10') AS is_spring10
    FROM valid
    WHERE SIZE(coupon_codes) >= 1
)
-- Grouped metrics
SELECT
    CASE WHEN coupon_codes_count_num_filled >= 72 THEN 'PREMIUM' ELSE 'STANDARD' END AS coupon_codes_count_num_filled_tier,
    COUNT(DISTINCT id) AS distinct_ids,
    SUM(coupon_codes_count_num_filled) AS total_coupon_codes_count_num_filled,
    ROUND(AVG(coupon_codes_count_num_filled), 2) AS avg_coupon_codes_count_num_filled,
    PERCENTILE_APPROX(coupon_codes_count_num_filled, 0.95) AS coupon_codes_count_num_filled_p95,
    COUNT(DISTINCT id) AS distinct_ids_2
FROM cleaned
GROUP BY coupon_codes_count_num_filled_tier
ORDER BY coupon_codes_count_num_filled_tier ASC;
