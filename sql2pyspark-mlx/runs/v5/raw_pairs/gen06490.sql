WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) e AS tier_value
)
SELECT id, tier_value
FROM lines
ORDER BY id NULLS FIRST;
