WITH flattened AS (SELECT id, plan, currency, EXPLODE(discounts) AS val FROM price_tiers) SELECT plan FROM flattened ORDER BY plan DESC NULLS FIRST;
