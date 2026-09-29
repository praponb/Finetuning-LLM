WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Ranking and gap features
    SELECT *, RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC) AS tier_value_rank
    FROM flattened
),
base AS (
    -- Basic cleansing and defaults
    SELECT *, CONCAT_WS(',', currency, plan) AS currency_plan_key
    FROM metrics
    WHERE plan = 'starter'
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        *,
        COALESCE(tier_value_rank, 0) AS tier_value_rank_filled,
        tier_value - tier_value_rank AS tier_value_minus_tier_value_rank
    FROM base
    WHERE plan != 'pro'
),
base_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        currency,
        tier_value,
        tier_value_rank,
        currency_plan_key,
        tier_value_rank_filled,
        LENGTH(currency) AS currency_length,
        tier_value * tier_value_rank AS tier_value_times_tier_value_rank
    FROM valid
    WHERE tier_value_rank < 5
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        currency,
        tier_value_rank,
        currency_plan_key,
        tier_value_rank_filled,
        currency_length,
        tier_value_times_tier_value_rank,
        CONCAT(currency, ' ', plan) AS currency_plan,
        ROUND(tier_value_rank, 1) AS tier_value_rank_rounded
    FROM base_price_tiers
    WHERE tier_value_rank BETWEEN 4 AND 6
)
-- Native SQL UNPIVOT (wide to long)
SELECT
    currency,
    kpi,
    val
FROM (
    SELECT
        currency,
        tier_value_times_tier_value_rank,
        tier_value_rank_rounded,
        tier_value_rank_filled
    FROM cleaned
) src
UNPIVOT INCLUDE NULLS (
    val FOR kpi IN (tier_value_times_tier_value_rank, tier_value_rank_rounded AS TIER_VALUE_RANK_ROUNDED, tier_value_rank_filled AS TIER_VALUE_RANK_FILLED)
) long_rows;
