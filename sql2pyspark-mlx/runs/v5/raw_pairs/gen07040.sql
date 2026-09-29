SELECT id, ends_on, discount_pct FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY promo_code ORDER BY discount_pct DESC, id) AS rn FROM promotions) x WHERE rn = 1
