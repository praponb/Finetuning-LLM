select id, plan, effective_from, currency, val from price_tiers lateral view explode(tier_prices) x as val
