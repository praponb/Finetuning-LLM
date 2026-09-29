WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
)
SELECT
    plan,
    effective_from,
    currency,
    tier_value
FROM flattened
