WITH lines AS (SELECT id, currency, EXPLODE_OUTER(discounts) AS tier_value FROM price_tiers) SELECT id, currency, tier_value FROM lines ORDER BY currency DESC
