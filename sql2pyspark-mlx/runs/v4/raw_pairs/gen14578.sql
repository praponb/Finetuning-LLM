SELECT id, plan, effective_from, currency, EXPLODE_OUTER(tier_prices) AS tier_value FROM price_tiers
