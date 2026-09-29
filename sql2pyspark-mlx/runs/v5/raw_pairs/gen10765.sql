SELECT
    id,
    currency,
    EXPLODE_OUTER(discounts) AS element
FROM price_tiers
