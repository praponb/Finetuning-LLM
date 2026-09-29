SELECT
    id,
    plan,
    effective_from,
    currency,
    val
FROM price_tiers
LATERAL VIEW EXPLODE(discounts) t AS val;
