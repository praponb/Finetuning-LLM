WITH flattened AS (SELECT id, effective_from, EXPLODE_OUTER(tier_prices) AS val FROM price_tiers) SELECT id, effective_from, val FROM flattened ORDER BY val ASC;
