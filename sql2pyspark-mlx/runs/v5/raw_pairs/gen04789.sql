WITH flattened AS (
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(discounts) AS val
    FROM price_tiers
)
SELECT id, effective_from
FROM flattened
ORDER BY id NULLS LAST
