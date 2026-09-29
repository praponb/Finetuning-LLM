WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        element
    FROM price_tiers
    LATERAL VIEW EXPLODE(discounts) li AS element
),
base AS (
    -- Keep recent, valid rows
    SELECT
        *,
        TRIM(currency) clean_currency,
        UPPER(currency) currency_upper
    FROM lines
    WHERE plan = 'pro'
),
base_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT *, LENGTH(currency) currency_length
    FROM base
    WHERE element IS NOT NULL
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        currency,
        element,
        clean_currency,
        CASE WHEN currency IN ('THB', 'USD') THEN 'Tier 1' ELSE 'Other' END currency_group
    FROM base_price_tiers
    WHERE element >= 52
)
SELECT
    id,
    plan,
    currency,
    element,
    currency_group,
    CASE WHEN currency RLIKE '^(THB|EUR)$' THEN 'GROUP_B' WHEN currency RLIKE '^(USD)$' THEN 'CORE' ELSE 'PRIORITY' END AS currency_zone
FROM valid
ORDER BY element DESC
