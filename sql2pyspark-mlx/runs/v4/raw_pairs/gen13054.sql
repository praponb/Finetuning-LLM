WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS element
    FROM price_tiers
)
SELECT
    id,
    plan,
    effective_from,
    currency,
    CASE WHEN plan RLIKE '^(pro|starter)$' THEN 'EXTENDED' WHEN plan RLIKE '^(basic|enterprise)$' THEN 'CORE' ELSE 'PRIORITY' END AS plan_zone
FROM flattened
ORDER BY plan_zone DESC NULLS FIRST, id DESC
