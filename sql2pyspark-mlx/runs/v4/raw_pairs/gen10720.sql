WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT id, tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS tier_value
)
SELECT tier_value
FROM flattened
ORDER BY tier_value NULLS LAST;
