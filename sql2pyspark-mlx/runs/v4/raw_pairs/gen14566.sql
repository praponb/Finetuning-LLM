WITH lines AS (
    SELECT
        id,
        plan,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS val
)
SELECT
    id,
    plan,
    val
FROM lines
