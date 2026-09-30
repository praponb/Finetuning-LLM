SELECT id, plan, tier_value FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_limits) x AS tier_value;
