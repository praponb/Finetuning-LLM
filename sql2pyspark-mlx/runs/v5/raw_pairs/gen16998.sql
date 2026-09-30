WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) e AS tier_value
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST) AS tier_value_rank,
        AVG(tier_value) OVER (PARTITION BY currency) AS currency_avg_tier_value,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value ASC NULLS LAST) AS tier_value_dense_rank
    FROM lines
    WHERE currency NOT IN ('EUR', 'USD')
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        tier_value_rank,
        tier_value_dense_rank / NULLIF(currency_avg_tier_value, 0) AS tier_value_dense_rank_per_currency_avg_tier_value
    FROM scored
    WHERE tier_value_rank != 6
),
remaining_keys AS (
    -- Set difference on keys
    SELECT plan
    FROM valid
    EXCEPT DISTINCT
    SELECT plan
    FROM price_tiers
    WHERE currency IN ('USD', 'EUR')
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM valid s
    JOIN remaining_keys k ON s.plan = k.plan
),
valid_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        UPPER(plan) AS plan_upper,
        LENGTH(plan) AS plan_length
    FROM verified
    WHERE tier_value_rank != 5
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        plan,
        tier_value_rank,
        ROUND(tier_value_rank / SUM(tier_value_rank) OVER (PARTITION BY plan), 4) AS tier_value_rank_share,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value_dense_rank_per_currency_avg_tier_value DESC NULLS LAST) AS tier_value_dense_rank_per_currency_avg_tier_value_dense_rank
    FROM valid_price_tiers
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LENGTH(plan) AS plan_length,
        ROUND(tier_value_dense_rank_per_currency_avg_tier_value_dense_rank * 100, 2) AS tier_value_dense_rank_per_currency_avg_tier_value_dense_rank_adjusted
    FROM scored_price_tiers
    WHERE plan IN ('pro', 'enterprise')
)
SELECT
    plan,
    tier_value_rank,
    tier_value_rank_share,
    tier_value_dense_rank_per_currency_avg_tier_value_dense_rank,
    tier_value_dense_rank_per_currency_avg_tier_value_dense_rank_adjusted
FROM filtered
ORDER BY tier_value_rank_share DESC NULLS FIRST, tier_value_dense_rank_per_currency_avg_tier_value_dense_rank DESC;
