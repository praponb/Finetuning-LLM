WITH lines AS (SELECT id, effective_from, currency, EXPLODE_OUTER(discounts) AS tier_value FROM price_tiers) SELECT id, effective_from, currency FROM lines ORDER BY id DESC
