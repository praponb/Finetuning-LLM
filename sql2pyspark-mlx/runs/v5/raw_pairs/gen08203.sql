SELECT id, effective_from, tier_value FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_prices) e AS tier_value
