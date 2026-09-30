SELECT id, plan, effective_from, currency, tier_value FROM price_tiers LATERAL VIEW EXPLODE(tier_prices) x AS tier_value;
