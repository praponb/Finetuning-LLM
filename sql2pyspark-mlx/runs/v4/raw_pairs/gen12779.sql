SELECT id, plan, effective_from, tier_value FROM price_tiers LATERAL VIEW EXPLODE(discounts) li AS tier_value
