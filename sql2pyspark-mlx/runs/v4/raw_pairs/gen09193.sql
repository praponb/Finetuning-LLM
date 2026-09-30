WITH exploded AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        k,
        value
    FROM carts
    LATERAL VIEW EXPLODE(attributes) e AS k, value
)
SELECT
    id,
    created_at,
    coupon_codes,
    k,
    value,
    FIRST_VALUE(k) IGNORE NULLS OVER (PARTITION BY k ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS first_known_k,
    ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at DESC, id, k) AS seq_no,
    ROW_NUMBER() OVER (PARTITION BY k ORDER BY created_at, id, k) AS row_seq,
    LAST_VALUE(k) IGNORE NULLS OVER (PARTITION BY k ORDER BY created_at, id, k ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS last_known_k
FROM exploded;
