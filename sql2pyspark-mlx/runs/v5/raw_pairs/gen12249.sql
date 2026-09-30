SELECT
    id,
    plan,
    effective_from,
    currency,
    element
FROM price_tiers
LATERAL VIEW EXPLODE(discounts) x AS element
