SELECT
    id,
    plan,
    effective_from,
    currency,
    val
FROM price_tiers
LATERAL VIEW OUTER EXPLODE(tier_limits) li AS val;
