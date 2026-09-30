WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
metrics AS (
    -- Ranking and gap features
    SELECT
        plan,
        effective_from,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val) AS val_dense_rank,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val,
        STDDEV_SAMP(val) OVER (PARTITION BY plan) AS plan_stddev_samp_val
    FROM lines
),
kept_keys AS (
    -- Set intersection on keys
    SELECT plan
    FROM metrics
    INTERSECT ALL
    SELECT plan
    FROM price_tiers
    WHERE plan != 'basic'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM metrics s
    JOIN kept_keys k ON s.plan = k.plan
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, GREATEST(plan_stddev_samp_val, plan_count_val) AS greatest_plan_stddev_samp_val_plan_count_val
    FROM verified
    WHERE effective_from >= DATE '2024-02-12'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_count_val DESC NULLS LAST) AS plan_count_val_percent_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val_dense_rank NULLS LAST) AS val_dense_rank_percent_rank,
        ROUND(plan_count_val / SUM(plan_count_val) OVER (PARTITION BY plan), 4) AS plan_count_val_share,
        ROUND(plan_count_val / SUM(plan_count_val) OVER (PARTITION BY plan), 4) AS plan_count_val_share_2
    FROM base
    WHERE greatest_plan_stddev_samp_val_plan_count_val > 34
),
enriched AS (
    -- Running and moving aggregates
    SELECT *, RANK() OVER (PARTITION BY plan ORDER BY plan_stddev_samp_val DESC NULLS LAST) AS plan_stddev_samp_val_rank
    FROM with_windows
)
SELECT
    plan,
    effective_from,
    plan_stddev_samp_val,
    plan_count_val_percent_rank,
    val_dense_rank_percent_rank,
    plan_count_val_share,
    plan_count_val_share_2,
    plan_stddev_samp_val_rank
FROM enriched
ORDER BY plan_stddev_samp_val NULLS LAST;
