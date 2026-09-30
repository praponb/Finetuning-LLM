SELECT id, plan, EXPLODE(discounts) AS val FROM price_tiers;
