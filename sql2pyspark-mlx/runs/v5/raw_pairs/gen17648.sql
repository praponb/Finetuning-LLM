WITH lines AS (SELECT id, plan, effective_from, tier_value FROM price_tiers LATERAL VIEW EXPLODE(tier_limits) t AS tier_value) SELECT plan, tier_value FROM lines ORDER BY plan;
