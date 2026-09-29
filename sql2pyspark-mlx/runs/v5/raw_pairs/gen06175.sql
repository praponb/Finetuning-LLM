SELECT id, plan, EXPLODE_OUTER(tier_limits) AS element FROM price_tiers;
