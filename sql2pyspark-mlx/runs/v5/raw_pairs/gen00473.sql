SELECT
    id,
    plan,
    currency,
    EXPLODE_OUTER(discounts) AS tier_value
FROM price_tiers
