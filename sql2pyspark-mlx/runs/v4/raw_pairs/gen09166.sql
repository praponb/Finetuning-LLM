WITH flattened AS (
    SELECT
        id,
        customer_id,
        created_at,
        EXPLODE(coupon_codes) AS tag
    FROM carts
)
SELECT
    id,
    customer_id,
    created_at,
    tag,
    CASE WHEN tag RLIKE '^(BUNDLE|VIP)$' THEN 'PRIORITY' WHEN tag RLIKE '^(SPRING10|WELCOME5)$' THEN 'OTHER_ZONE' ELSE 'CORE' END AS tag_zone
FROM flattened;
