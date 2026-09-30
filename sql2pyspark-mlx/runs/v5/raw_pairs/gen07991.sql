WITH exploded AS (SELECT id, plan, currency, EXPLODE_OUTER(discounts) AS element FROM price_tiers) SELECT id FROM exploded ORDER BY id DESC NULLS LAST;
