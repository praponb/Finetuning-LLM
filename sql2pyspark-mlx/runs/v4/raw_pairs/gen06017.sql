WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) x AS element
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, TRIM(plan) clean_plan
    FROM exploded
    WHERE element >= 64
),
base_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        currency,
        element,
        clean_plan,
        CASE WHEN currency IN ('EUR', 'USD') THEN 'Core' ELSE 'Other' END currency_group,
        CASE WHEN element >= 80 THEN 'High' WHEN element >= 26 THEN 'Medium' ELSE 'Low' END element_tier
    FROM base
    WHERE plan IN ('basic', 'starter', 'pro')
)
SELECT
    id,
    plan,
    currency,
    clean_plan,
    currency_group,
    CASE WHEN currency RLIKE '^(USD|EUR)$' THEN 'PRIORITY' WHEN currency RLIKE '^(THB)$' THEN 'GROUP_A' ELSE 'GROUP_B' END AS currency_zone
FROM base_price_tiers
