WITH flattened AS (SELECT id, currency, EXPLODE(discounts) AS val FROM price_tiers) SELECT id, val FROM flattened ORDER BY id DESC;
