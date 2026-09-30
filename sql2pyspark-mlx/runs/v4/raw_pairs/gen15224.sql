WITH exploded AS (
    SELECT
        id,
        customer_id,
        EXPLODE_OUTER(coupon_codes) AS tag
    FROM carts
)
SELECT tag, CASE WHEN tag RLIKE '^(VIP|WELCOME5)$' THEN 'GROUP_B' ELSE 'PRIORITY' END AS tag_zone
FROM exploded
ORDER BY tag DESC;
