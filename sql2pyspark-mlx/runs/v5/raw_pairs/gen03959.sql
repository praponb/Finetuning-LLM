WITH lines AS (SELECT id, plan, currency, EXPLODE(tier_prices) AS element FROM price_tiers) SELECT id, currency, element FROM lines ORDER BY element DESC, id;
