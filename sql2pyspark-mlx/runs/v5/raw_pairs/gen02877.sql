WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS element
    FROM carts
)
SELECT id, created_at
FROM lines
ORDER BY created_at DESC
