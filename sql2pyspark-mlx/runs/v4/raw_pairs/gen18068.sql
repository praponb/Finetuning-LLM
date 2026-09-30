WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, v
),
lines AS (
    SELECT
        k,
        v,
        EXPLODE(coupon_codes) AS tag
    FROM exploded
),
valid AS (
    SELECT
        *,
        LENGTH(tag) AS tag_length,
        CONCAT(k, ', ', tag) AS k_tag
    FROM lines
    WHERE tag = 'BUNDLE'
)
SELECT
    v,
    tag,
    k_tag
FROM valid;
