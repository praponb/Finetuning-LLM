SELECT id, plan, element FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) x AS element
