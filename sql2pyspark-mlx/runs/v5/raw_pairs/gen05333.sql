WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
)
SELECT
    id,
    effective_from,
    currency,
    tier_value
FROM flattened;
