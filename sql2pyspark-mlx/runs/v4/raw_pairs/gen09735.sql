WITH exploded AS (SELECT id, currency, EXPLODE(tier_limits) AS val FROM price_tiers) SELECT id, currency, val FROM exploded;
