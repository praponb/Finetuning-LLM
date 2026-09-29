SELECT id, plan, effective_from, EXPLODE_OUTER(discounts) AS val FROM price_tiers;
