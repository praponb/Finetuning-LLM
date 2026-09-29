WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        customer_id,
        created_at,
        coupon_codes,
        k,
        attr_value
    FROM carts
    LATERAL VIEW OUTER EXPLODE(attributes) t AS k, attr_value
)
SELECT
    s.customer_id,
    s.coupon_codes,
    s.k,
    s.attr_value,
    CASE WHEN k RLIKE '^(weight_kg|gift)$' THEN 'OTHER_ZONE' ELSE 'GROUP_A' END AS k_zone,
    (SELECT SUM(s2.monthly_fee) FROM subscriptions s2 WHERE s2.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM exploded s
ORDER BY k_zone DESC NULLS LAST
