WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) t AS tier_value
)
SELECT
    id,
    plan,
    tier_value
FROM lines
ORDER BY plan DESC NULLS LAST
