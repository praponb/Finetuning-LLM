WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        coupon_codes,
        key,
        v
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS key, v
)
SELECT
    id,
    coupon_codes,
    key,
    v,
    CASE WHEN key RLIKE '^(gift|utm_source)$' THEN 'EXTENDED' ELSE 'OTHER_ZONE' END AS key_zone
FROM exploded
