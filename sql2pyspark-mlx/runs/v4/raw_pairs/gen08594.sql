WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
)
SELECT
    id,
    plan,
    effective_from,
    val
FROM exploded
