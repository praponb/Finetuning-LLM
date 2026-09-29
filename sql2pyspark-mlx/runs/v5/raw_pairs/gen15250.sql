WITH exploded AS (
    SELECT
        src.id,
        src.customer_id,
        src.created_at,
        src.coupon_codes,
        entry.qty,
        entry.promo
    FROM carts src
    LATERAL VIEW OUTER EXPLODE(src.items) t AS entry
)
SELECT
    s.customer_id,
    s.qty,
    s.promo,
    CASE WHEN promo RLIKE '^(BUNDLE|WELCOME5)$' THEN 'GROUP_A' WHEN promo RLIKE '^(SPRING10|VIP)$' THEN 'CORE' ELSE 'OTHER_ZONE' END AS promo_zone,
    (SELECT SUM(x.monthly_fee) FROM subscriptions x WHERE x.customer_id = s.customer_id) AS sum_subscription_monthly_fee
FROM exploded s
ORDER BY promo DESC
