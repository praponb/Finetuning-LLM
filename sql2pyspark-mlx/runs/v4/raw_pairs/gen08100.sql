WITH lines AS (SELECT id, currency, val FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_limits) t AS val) SELECT id, currency, val FROM lines ORDER BY val DESC;
