SELECT id, plan, EXPLODE_OUTER(tier_limits) AS val FROM price_tiers;
