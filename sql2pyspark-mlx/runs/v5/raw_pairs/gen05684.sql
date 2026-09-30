WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) t AS element
)
SELECT id, plan
FROM flattened
ORDER BY id DESC;
