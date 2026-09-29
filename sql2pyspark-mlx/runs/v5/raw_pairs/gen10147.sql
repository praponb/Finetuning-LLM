SELECT id, effective_from, currency, element FROM price_tiers LATERAL VIEW EXPLODE(tier_prices) e AS element
