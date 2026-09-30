WITH matched AS (
    -- Non-equi join on a validity window
    SELECT
        s.*,
        pr.id AS promo_id,
        pr.promo_code AS promotion_promo_code,
        pr.discount_pct AS promotion_discount_pct
    FROM promotions s
    INNER JOIN promotions pr
        ON COALESCE(s.channel, 'web') = pr.channel
       AND s.starts_on >= pr.starts_on
       AND s.starts_on <= DATE_ADD(pr.ends_on, 3)
)
SELECT
    id,
    channel,
    starts_on,
    ends_on,
    promo_id,
    promotion_promo_code,
    promotion_discount_pct
FROM matched
ORDER BY promo_id NULLS LAST, ends_on;
