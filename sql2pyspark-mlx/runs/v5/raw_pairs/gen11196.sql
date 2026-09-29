SELECT
    id,
    plan,
    currency,
    EXPLODE_OUTER(discounts) AS element
FROM price_tiers;
