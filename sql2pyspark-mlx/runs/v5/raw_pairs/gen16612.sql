WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_limits) AS element
    FROM price_tiers
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, DAYOFWEEK(effective_from) AS effective_from_dayofweek
    FROM flattened
    WHERE plan NOT IN ('pro', 'starter')
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, SUBSTRING(currency, 1, 3) AS currency_prefix
    FROM base
    WHERE currency NOT IN ('THB', 'EUR')
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(element) OVER (PARTITION BY currency, plan) AS currency_sum_element,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY effective_from_dayofweek DESC) AS effective_from_dayofweek_percent_rank
    FROM valid
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        currency,
        plan,
        effective_from_dayofweek
    FROM enriched
) src
PIVOT (
    MAX(effective_from_dayofweek)
    FOR plan IN ('pro' AS pro, 'starter' AS starter, 'enterprise' AS enterprise)
);
