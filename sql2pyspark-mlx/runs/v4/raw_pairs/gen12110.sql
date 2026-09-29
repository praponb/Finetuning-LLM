WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
)
SELECT id, plan
FROM exploded
ORDER BY plan DESC
