WITH lines AS (
    SELECT
        id,
        created_at,
        coupon_codes,
        attr_key,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS attr_key, attr_value
)
SELECT
    id,
    coupon_codes,
    attr_key,
    CASE WHEN attr_key RLIKE '^(weight_kg|device)$' THEN 'PRIORITY' WHEN attr_key RLIKE '^(utm_source|gift)$' THEN 'EXTENDED' ELSE 'GROUP_B' END AS attr_key_zone
FROM lines
ORDER BY attr_key DESC, attr_key_zone DESC;
