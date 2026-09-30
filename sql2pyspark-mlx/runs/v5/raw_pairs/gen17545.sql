WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) e AS tier_value
),
enriched AS (
    -- Ranking and gap features
    SELECT *, CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist
    FROM exploded
    WHERE plan = 'enterprise'
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        effective_from,
        tier_value,
        tier_value_cume_dist,
        DATE_SUB(effective_from, 65) AS effective_from_minus_65d
    FROM enriched
    WHERE effective_from > DATE '2024-03-17'
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, WEEKOFYEAR(effective_from) AS effective_from_weekofyear
    FROM cleaned
    WHERE effective_from < '2025-09-01'
)
SELECT
    id,
    plan,
    effective_from,
    effective_from_minus_65d,
    effective_from_weekofyear
FROM base
ORDER BY id NULLS FIRST;
