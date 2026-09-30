WITH lines AS (
    SELECT
        id,
        created_at,
        EXPLODE(coupon_codes) AS tag
    FROM carts
)
SELECT
    id,
    created_at,
    tag
FROM lines;
