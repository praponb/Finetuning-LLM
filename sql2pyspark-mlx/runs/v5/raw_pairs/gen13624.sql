WITH flattened AS (SELECT id, effective_from, EXPLODE(tier_limits) AS element FROM price_tiers) SELECT id, element FROM flattened;
