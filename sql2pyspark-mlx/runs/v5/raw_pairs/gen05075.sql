WITH flattened AS (
    SELECT
        id,
        customer_id,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
)
SELECT customer_id, tag
FROM flattened
ORDER BY customer_id DESC NULLS FIRST
