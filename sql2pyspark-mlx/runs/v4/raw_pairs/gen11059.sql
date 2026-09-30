WITH exploded AS (SELECT id, val FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) li AS val) SELECT id, val FROM exploded ORDER BY val DESC NULLS FIRST
