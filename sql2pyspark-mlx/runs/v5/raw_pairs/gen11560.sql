SELECT id, effective_from, currency, EXPLODE_OUTER(discounts) AS element FROM price_tiers
