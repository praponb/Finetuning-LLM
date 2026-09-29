WITH flattened AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
)
SELECT
    id,
    effective_from,
    currency
FROM flattened
ORDER BY id DESC NULLS FIRST, currency DESC
