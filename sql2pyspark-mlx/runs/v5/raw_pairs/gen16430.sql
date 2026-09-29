WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS tier_value
    FROM price_tiers
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        effective_from,
        tier_value,
        CONCAT(plan, '/', currency) AS plan_currency
    FROM exploded
    WHERE tier_value IS NOT NULL
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, COALESCE(tier_value, 0) AS tier_value_filled
    FROM cleaned
    WHERE effective_from > '2025-09-01'
),
valid_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, tier_value * tier_value_filled AS tier_value_times_tier_value_filled
    FROM valid
    WHERE effective_from >= DATE '2024-05-25'
      AND effective_from < '2025-05-01'
)
-- Ranking and gap features
SELECT
    id,
    plan,
    effective_from,
    tier_value,
    tier_value_filled,
    PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank,
    STDDEV_SAMP(tier_value_times_tier_value_filled) OVER (PARTITION BY plan) AS plan_stddev_samp_tier_value_times_tier_value_filled,
    DENSE_RANK() OVER (ORDER BY tier_value_times_tier_value_filled DESC NULLS FIRST) AS tier_value_times_tier_value_filled_dense_rank
FROM valid_price_tiers
WHERE tier_value_times_tier_value_filled >= 29;
