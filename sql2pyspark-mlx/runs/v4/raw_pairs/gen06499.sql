SELECT id, plan, effective_from, currency, EXPLODE_OUTER(tier_prices) AS val FROM price_tiers;
