select id, plan, effective_from, tier_value from price_tiers lateral view explode(tier_prices) e as tier_value;
