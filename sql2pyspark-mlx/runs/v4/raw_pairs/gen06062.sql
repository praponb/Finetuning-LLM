WITH lines AS (SELECT id, currency, tier_value FROM price_tiers LATERAL VIEW OUTER EXPLODE(tier_prices) e AS tier_value) SELECT id, tier_value FROM lines ORDER BY tier_value DESC;
