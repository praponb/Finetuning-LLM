WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS val
)
SELECT id, currency
FROM lines
