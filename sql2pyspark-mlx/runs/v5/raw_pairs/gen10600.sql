SELECT id, plan, element FROM price_tiers LATERAL VIEW OUTER EXPLODE(discounts) x AS element
