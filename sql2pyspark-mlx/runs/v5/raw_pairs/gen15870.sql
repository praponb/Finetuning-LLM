WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Running and moving aggregates
    SELECT *, DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value NULLS LAST) AS tier_value_dense_rank
    FROM lines
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, LOWER(plan) AS plan_lower
    FROM scored
    WHERE currency = 'THB'
),
valid_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        tier_value,
        tier_value_dense_rank,
        plan_lower,
        ROUND(tier_value_dense_rank * 1.1, 2) AS adjusted_tier_value_dense_rank,
        CEIL(tier_value) AS tier_value_whole
    FROM valid
    WHERE tier_value = 68
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        tier_value_dense_rank,
        adjusted_tier_value_dense_rank,
        CEIL(adjusted_tier_value_dense_rank) AS adjusted_tier_value_dense_rank_whole,
        tier_value - adjusted_tier_value_dense_rank AS tier_value_minus_adjusted_tier_value_dense_rank
    FROM valid_price_tiers
    WHERE tier_value > 42
),
base_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        tier_value_dense_rank,
        adjusted_tier_value_dense_rank_whole,
        tier_value_minus_adjusted_tier_value_dense_rank,
        adjusted_tier_value_dense_rank_whole - tier_value_minus_adjusted_tier_value_dense_rank AS adjusted_tier_value_dense_rank_whole_minus_tier_value_minus_adjusted_tier_value_dense_rank,
        adjusted_tier_value_dense_rank / NULLIF(tier_value_minus_adjusted_tier_value_dense_rank, 0) AS adjusted_tier_value_dense_rank_per_tier_value_minus_adjusted_tier_value_dense_rank
    FROM base
    WHERE adjusted_tier_value_dense_rank > 32
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id, plan
    FROM base_price_tiers
    INTERSECT
    SELECT id, plan
    FROM price_tiers
    WHERE currency IN ('USD', 'EUR')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM base_price_tiers s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.plan = k.plan
),
grouped AS (
    -- Grouped metrics
    SELECT
        plan,
        CASE WHEN tier_value_minus_adjusted_tier_value_dense_rank >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_minus_adjusted_tier_value_dense_rank_tier,
        COUNT_IF(plan = 'starter') AS starter_events,
        COUNT_IF(plan = 'starter') AS starter_events_2,
        ROUND(SUM(adjusted_tier_value_dense_rank_per_tier_value_minus_adjusted_tier_value_dense_rank) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS adjusted_tier_value_dense_rank_per_tier_value_minus_adjusted_tier_value_dense_rank_per_pro,
        ROUND(SUM(tier_value_minus_adjusted_tier_value_dense_rank) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS tier_value_minus_adjusted_tier_value_dense_rank_per_basic
    FROM kept_rows
    GROUP BY GROUPING SETS ((plan, CASE WHEN tier_value_minus_adjusted_tier_value_dense_rank >= 76 THEN 'PREMIUM' ELSE 'STANDARD' END), (plan))
)
SELECT
    tier_value_minus_adjusted_tier_value_dense_rank_tier,
    starter_events,
    adjusted_tier_value_dense_rank_per_tier_value_minus_adjusted_tier_value_dense_rank_per_pro,
    tier_value_minus_adjusted_tier_value_dense_rank_per_basic
FROM grouped
ORDER BY starter_events, adjusted_tier_value_dense_rank_per_tier_value_minus_adjusted_tier_value_dense_rank_per_pro DESC;
