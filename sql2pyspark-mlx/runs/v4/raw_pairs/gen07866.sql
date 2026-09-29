WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        code
    FROM carts
    LATERAL VIEW OUTER EXPLODE(coupon_codes) t AS code
)
-- Grouped metrics
SELECT
    code,
    customer_id,
    COUNT(*) FILTER (WHERE code = 'VIP') AS vip_rows,
    COUNT(*) FILTER (WHERE code = 'WELCOME5') AS welcome5_rows,
    SORT_ARRAY(COLLECT_SET(code)) AS code_list
FROM exploded
GROUP BY code, customer_id;
