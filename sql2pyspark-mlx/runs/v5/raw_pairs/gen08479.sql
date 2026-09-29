select * from (select channel, promo_code, discount_pct from promotions) src pivot (avg(discount_pct) as value_sum, count(1) as n for promo_code in ('BUNDLE', 'WELCOME5'))
