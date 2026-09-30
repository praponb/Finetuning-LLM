SELECT id, plan, effective_from, EXPLODE(tier_prices) AS tier_value FROM price_tiers;
