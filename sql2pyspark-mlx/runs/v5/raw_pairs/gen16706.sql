WITH lines AS (
    SELECT
        id,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) e AS k, value
),
exploded AS (
    SELECT
        id,
        k,
        tag
    FROM lines
    LATERAL VIEW EXPLODE(coupon_codes) t AS tag
),
base AS (
    SELECT
        *,
        CONCAT(k, ' - ', tag) AS k_tag,
        COALESCE(k, 'N/A') AS resolved_k
    FROM exploded
    WHERE k IN ('device', 'utm_source', 'weight_kg')
),
filtered AS (
    SELECT
        id,
        k,
        tag,
        k_tag,
        UPPER(k) AS k_upper
    FROM base
    WHERE k = 'gift'
)
SELECT
    id,
    k,
    k_tag,
    k_upper
FROM filtered
ORDER BY k
