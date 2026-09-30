WITH lines AS (SELECT id, plan, currency, tier_value FROM price_tiers LATERAL VIEW EXPLODE(tier_prices) e AS tier_value) SELECT id, tier_value FROM lines ORDER BY id DESC NULLS LAST;
