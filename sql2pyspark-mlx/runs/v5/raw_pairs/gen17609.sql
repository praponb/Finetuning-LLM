WITH flattened AS (SELECT id, plan, currency, element FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_limits) e AS element) SELECT id, currency, element FROM flattened ORDER BY currency NULLS LAST;
