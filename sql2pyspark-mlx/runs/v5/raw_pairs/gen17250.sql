SELECT
    id,
    effective_from,
    currency,
    EXPLODE_OUTER(discounts) AS val
FROM price_tiers;
