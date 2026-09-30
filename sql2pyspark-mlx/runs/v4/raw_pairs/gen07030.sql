SELECT id, plan, effective_from, currency, element FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) x AS element;
