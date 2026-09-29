WITH exploded AS (SELECT id, effective_from, currency, EXPLODE_OUTER(tier_prices) AS val FROM price_tiers) SELECT id, currency, val FROM exploded;
