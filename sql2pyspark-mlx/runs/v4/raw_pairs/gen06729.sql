WITH lines AS (
    SELECT
        id,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS tier_value
    FROM price_tiers
),
valid AS (
    SELECT *, TRIM(currency) clean_currency
    FROM lines
    WHERE effective_from <= '2025-02-01'
),
base AS (
    SELECT
        *,
        ROUND(tier_value * 1.15, 2) adjusted_tier_value,
        WEEKOFYEAR(effective_from) effective_from_weekofyear
    FROM valid
    WHERE currency = 'USD'
),
report AS (
    SELECT
        id,
        effective_from,
        tier_value,
        clean_currency,
        adjusted_tier_value
    FROM base
    ORDER BY effective_from, id DESC
)
SELECT
    effective_from,
    tier_value,
    adjusted_tier_value
FROM report;
