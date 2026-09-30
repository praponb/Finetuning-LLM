WITH with_window AS (
    SELECT
        a.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions a
    LEFT JOIN promotions pr
        ON a.channel <=> pr.channel
       AND a.starts_on >= pr.starts_on
       AND a.starts_on < pr.ends_on + INTERVAL '7' DAY
)
SELECT
    id,
    ends_on,
    discount_pct,
    promo_id,
    promotion_promo_code
FROM with_window
ORDER BY promo_id DESC NULLS FIRST;
