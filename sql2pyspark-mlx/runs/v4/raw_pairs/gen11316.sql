WITH flattened AS (
    SELECT
        id,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW EXPLODE(attributes) li AS k, v
),
filtered AS (
    SELECT *, SUBSTRING(k, 1, 2) AS k_prefix
    FROM flattened
    WHERE ARRAY_CONTAINS(coupon_codes, 'BUNDLE')
),
valid AS (
    SELECT
        id,
        coupon_codes,
        k,
        v,
        k_prefix,
        CASE WHEN k = 'device' THEN 1 ELSE 0 END AS is_device
    FROM filtered
    WHERE k IN ('utm_source', 'gift')
),
metrics AS (
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY is_device DESC) AS is_device_dense_rank,
        LAST_VALUE(k) IGNORE NULLS OVER (ORDER BY is_device, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k
    FROM valid
)
SELECT
    id,
    coupon_codes,
    k,
    v,
    k_prefix,
    is_device_dense_rank,
    last_known_k
FROM metrics;
