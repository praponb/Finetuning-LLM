select id, plan, currency, element from price_tiers lateral view outer explode(tier_prices) e as element
