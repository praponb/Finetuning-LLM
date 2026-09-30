WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS element
)
SELECT
    effective_from,
    currency,
    element
FROM exploded
ORDER BY effective_from DESC NULLS FIRST
