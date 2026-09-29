WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS element
)
SELECT id, element
FROM flattened
