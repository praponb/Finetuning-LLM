WITH lines AS (SELECT id, plan, effective_from, EXPLODE(tier_limits) AS val FROM price_tiers) SELECT id, plan, val FROM lines ORDER BY plan DESC
