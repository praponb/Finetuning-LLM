SELECT id, plan, currency, tier_value FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_limits) li AS tier_value;
