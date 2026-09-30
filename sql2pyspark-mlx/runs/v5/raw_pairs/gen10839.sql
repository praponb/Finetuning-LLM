WITH exploded AS (
    SELECT
        id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) x AS k, value
),
flattened AS (
    SELECT k, code
    FROM exploded
    LATERAL VIEW EXPLODE(coupon_codes) t AS code
),
filtered AS (
    SELECT *, TRIM(k) AS clean_k
    FROM flattened
    WHERE code = 'VIP'
),
cleaned AS (
    SELECT
        *,
        CASE WHEN code = 'BUNDLE' THEN 1 ELSE 0 END AS is_bundle,
        SUBSTRING(k, 1, 3) AS k_prefix
    FROM filtered
    WHERE k IS NOT NULL
      AND k IN ('utm_source', 'gift')
),
cleaned_carts AS (
    SELECT *, FLOOR(is_bundle) AS is_bundle_whole
    FROM cleaned
    WHERE code NOT IN ('VIP', 'SPRING10', 'WELCOME5')
),
valid AS (
    SELECT
        *,
        GREATEST(is_bundle_whole, is_bundle) AS greatest_is_bundle_whole_is_bundle,
        ROUND(is_bundle_whole, 1) AS is_bundle_whole_rounded
    FROM cleaned_carts
    WHERE k IS NOT NULL
      AND code IS DISTINCT FROM 'BUNDLE'
)
SELECT
    code,
    PERCENTILE_APPROX(greatest_is_bundle_whole_is_bundle, ARRAY(0.25, 0.5, 0.75)) AS greatest_is_bundle_whole_is_bundle_quartiles,
    COUNT(*) AS event_count,
    SUM(CASE WHEN code = 'BUNDLE' THEN is_bundle_whole ELSE 0 END) AS bundle_is_bundle_whole,
    COUNT(*) AS event_count_2,
    AVG(is_bundle) FILTER (WHERE code = 'SPRING10') AS spring10_avg_is_bundle
FROM valid
GROUP BY code
ORDER BY code NULLS FIRST;
