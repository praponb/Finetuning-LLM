SELECT id, val FROM price_tiers LATERAL VIEW OUTER EXPLODE(discounts) e AS val
