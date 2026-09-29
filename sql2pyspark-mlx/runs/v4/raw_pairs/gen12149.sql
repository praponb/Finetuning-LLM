WITH lines AS (SELECT id, EXPLODE(discounts) AS element FROM price_tiers) SELECT id, element FROM lines ORDER BY element ASC NULLS FIRST, id ASC;
