select id, plan, currency, explode(tier_limits) as val from price_tiers;
