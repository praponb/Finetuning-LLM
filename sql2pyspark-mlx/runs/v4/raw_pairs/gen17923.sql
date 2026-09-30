WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON e.channel <=> pr.channel
       AND e.starts_on >= pr.starts_on
       AND e.starts_on < pr.ends_on + INTERVAL 14 DAYS
)
SELECT
    id,
    promo_code,
    starts_on,
    ends_on,
    promo_id,
    promotion_discount_pct,
    CASE WHEN promo_code RLIKE '^(VIP|WELCOME5)$' THEN 'PRIORITY' WHEN promo_code RLIKE '^(SPRING10|BUNDLE)$' THEN 'CORE' ELSE 'GROUP_B' END AS promo_code_zone
FROM with_window
ORDER BY promo_code DESC NULLS FIRST, promotion_discount_pct
