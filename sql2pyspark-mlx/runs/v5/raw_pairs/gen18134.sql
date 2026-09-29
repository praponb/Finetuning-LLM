WITH with_window AS (
    -- Non-equi join on a validity window
    SELECT
        e.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions e
    LEFT JOIN promotions pr
        ON e.channel = pr.channel
       AND e.ends_on >= pr.starts_on
       AND e.ends_on < pr.ends_on + INTERVAL 30 DAYS
)
SELECT
    id,
    channel,
    promo_code,
    starts_on,
    ends_on,
    discount_pct,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct,
    CASE WHEN promo_code RLIKE '^(BUNDLE|SPRING10)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS promo_code_zone
FROM with_window
ORDER BY ends_on DESC NULLS FIRST
