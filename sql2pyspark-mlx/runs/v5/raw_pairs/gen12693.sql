with lines as (select id, plan, element from price_tiers lateral view explode(tier_prices) x as element) select id, plan, element from lines;
