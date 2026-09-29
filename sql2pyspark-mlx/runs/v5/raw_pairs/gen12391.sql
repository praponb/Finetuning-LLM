WITH lines AS (SELECT id, plan, effective_from, currency, EXPLODE_OUTER(tier_prices) AS val FROM price_tiers) SELECT id, plan, effective_from, currency, val FROM lines ORDER BY effective_from;
