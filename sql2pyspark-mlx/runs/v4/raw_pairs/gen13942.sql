SELECT * FROM (SELECT *, RANK() OVER (PARTITION BY promo_code, channel ORDER BY discount_pct DESC) AS rn FROM promotions) x WHERE rn = 1
