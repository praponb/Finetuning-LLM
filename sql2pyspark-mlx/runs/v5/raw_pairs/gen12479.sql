SELECT
    id,
    plan,
    effective_from,
    currency,
    val
FROM price_tiers
LATERAL VIEW EXPLODE(tier_prices) e AS val;
