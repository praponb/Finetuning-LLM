SELECT * FROM (SELECT promo_code, channel, discount_pct FROM promotions) src PIVOT (SUM(discount_pct) FOR channel IN ('web' AS web, 'mobile' AS mobile));
