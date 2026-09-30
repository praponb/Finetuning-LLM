SELECT * FROM (SELECT promo_code, channel, discount_pct FROM promotions) src PIVOT (MAX(discount_pct) AS amount FOR channel IN ('mobile' AS mobile, 'web' AS web));
