SELECT id, effective_from, tier_value FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) x AS tier_value;
