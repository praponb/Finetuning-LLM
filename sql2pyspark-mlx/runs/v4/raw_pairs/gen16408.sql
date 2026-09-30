WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) e AS code
)
SELECT
    customer_id,
    created_at,
    code
FROM flattened
ORDER BY created_at DESC NULLS FIRST;
