WITH flattened AS (SELECT id, currency, val FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) t AS val) SELECT id, val FROM flattened ORDER BY val DESC;
