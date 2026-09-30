SELECT id, effective_from, tier_value FROM price_tiers LATERAL VIEW OUTER EXPLODE(discounts) x AS tier_value
