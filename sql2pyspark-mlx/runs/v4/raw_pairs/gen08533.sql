with flattened as (select id, plan, element from price_tiers lateral view explode(tier_prices) li as element) select id from flattened;
