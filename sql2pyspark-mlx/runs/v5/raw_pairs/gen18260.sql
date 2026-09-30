with lines as (select id, plan, effective_from, currency, element from price_tiers lateral view explode(tier_prices) t as element) select effective_from, element from lines;
