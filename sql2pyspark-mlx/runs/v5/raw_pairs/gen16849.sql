SELECT
    id,
    plan,
    EXPLODE_OUTER(discounts) AS element
FROM price_tiers;
