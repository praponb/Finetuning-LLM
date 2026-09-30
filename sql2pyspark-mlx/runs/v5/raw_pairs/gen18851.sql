SELECT
    id,
    plan,
    effective_from,
    currency,
    element
FROM price_tiers
LATERAL VIEW EXPLODE(discounts) li AS element;
