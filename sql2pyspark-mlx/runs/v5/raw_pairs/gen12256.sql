WITH exploded AS (SELECT id, currency, element FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_prices) t AS element) SELECT id FROM exploded ORDER BY id DESC NULLS LAST;
