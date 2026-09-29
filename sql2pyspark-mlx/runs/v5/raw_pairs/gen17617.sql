SELECT * FROM (SELECT promo_code, channel, discount_pct FROM promotions) src PIVOT (AVG(discount_pct) AS amount, COUNT(1) AS cnt FOR channel IN ('store', 'partner'));
