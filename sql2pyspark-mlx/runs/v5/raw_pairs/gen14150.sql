WITH exploded AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE_OUTER(coupon_codes) AS element
    FROM carts
)
SELECT
    id,
    element,
    CASE WHEN element RLIKE '^(VIP|BUNDLE)$' THEN 'EXTENDED' ELSE 'GROUP_A' END AS element_zone
FROM exploded
ORDER BY id DESC NULLS LAST
