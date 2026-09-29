WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        created_at,
        coupon_codes,
        line.qty,
        line.promo
    FROM (
        SELECT
            id,
            created_at,
            coupon_codes,
            EXPLODE_OUTER(items) AS line
        FROM carts
    ) e
)
SELECT promo
FROM lines
ORDER BY promo DESC NULLS LAST
