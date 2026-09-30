SELECT id, plan, val FROM price_tiers LATERAL VIEW EXPLODE(discounts) x AS val;
