WITH exploded AS (SELECT id, effective_from, element FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_prices) li AS element) SELECT effective_from FROM exploded ORDER BY effective_from NULLS FIRST
