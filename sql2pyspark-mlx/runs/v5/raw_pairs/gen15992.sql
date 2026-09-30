WITH exploded AS (SELECT id, plan, element FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) t AS element) SELECT id, plan, element FROM exploded ORDER BY element DESC NULLS LAST
