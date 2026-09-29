WITH flattened AS (SELECT id, plan, currency, EXPLODE(tier_limits) AS val FROM price_tiers) SELECT plan, val, MAX(val) OVER (PARTITION BY plan) AS plan_max_val FROM flattened;
