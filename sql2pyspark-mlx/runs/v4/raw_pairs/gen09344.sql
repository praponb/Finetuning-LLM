SELECT * FROM (SELECT channel, promo_code, discount_pct FROM promotions) src PIVOT (MAX(discount_pct) AS total, COUNT(1) AS n FOR promo_code IN ('WELCOME5', 'BUNDLE'));
