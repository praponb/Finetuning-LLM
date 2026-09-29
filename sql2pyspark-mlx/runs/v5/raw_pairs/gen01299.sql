WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS tier_value
)
SELECT
    id,
    effective_from,
    tier_value
FROM flattened
ORDER BY tier_value DESC, id DESC
