WITH exploded AS (SELECT id, effective_from, currency, element FROM price_tiers LATERAL VIEW EXPLODE(discounts) x AS element) SELECT id, effective_from, currency FROM exploded
