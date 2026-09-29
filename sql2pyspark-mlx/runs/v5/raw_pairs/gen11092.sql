SELECT
    src.*,
    pr.id AS promo_id,
    pr.promo_code AS promotion_promo_code,
    pr.discount_pct AS promotion_discount_pct
FROM promotions src
LEFT JOIN promotions pr
    ON src.channel = pr.channel
   AND src.ends_on BETWEEN pr.starts_on AND pr.ends_on
