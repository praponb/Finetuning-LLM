SELECT
    id,
    effective_from,
    currency,
    element
FROM price_tiers
LATERAL VIEW EXPLODE(discounts) e AS element;
