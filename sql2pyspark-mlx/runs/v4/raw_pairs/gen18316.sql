WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        SUM(tier_value) OVER w_grp plan_sum_tier_value,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC) tier_value_cume_dist,
        STDDEV_SAMP(tier_value) OVER w_grp plan_stddev_samp_tier_value
    FROM exploded
    WINDOW w_grp AS (PARTITION BY plan)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(plan_stddev_samp_tier_value, 1) plan_stddev_samp_tier_value_rounded
    FROM with_windows
    WHERE tier_value BETWEEN 48 AND 76
),
with_windows_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(tier_value) OVER w_recent plan_stddev_samp_tier_value_2,
        AVG(plan_stddev_samp_tier_value_rounded) OVER w_recent plan_avg_plan_stddev_samp_tier_value_rounded,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value ASC NULLS FIRST) tier_value_rank,
        SUM(plan_stddev_samp_tier_value) OVER w_recent plan_sum_plan_stddev_samp_tier_value
    FROM base
    WINDOW w_recent AS (PARTITION BY plan)
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        COALESCE(tier_value, 0) tier_value_filled,
        CAST(plan_sum_plan_stddev_samp_tier_value AS DOUBLE) plan_sum_plan_stddev_samp_tier_value_num
    FROM with_windows_price_tiers
    WHERE plan IN ('starter', 'enterprise')
),
with_windows_price_tiers_2 AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        tier_value,
        plan_sum_tier_value,
        tier_value_cume_dist,
        plan_stddev_samp_tier_value,
        plan_stddev_samp_tier_value_rounded,
        plan_stddev_samp_tier_value_2,
        plan_avg_plan_stddev_samp_tier_value_rounded,
        tier_value_rank,
        plan_sum_plan_stddev_samp_tier_value,
        tier_value_filled,
        plan_sum_plan_stddev_samp_tier_value_num,
        AVG(plan_stddev_samp_tier_value_rounded) OVER w_hist plan_avg_plan_stddev_samp_tier_value_rounded_2,
        COUNT(tier_value_rank) OVER w_hist plan_count_tier_value_rank
    FROM valid
    WINDOW w_hist AS (PARTITION BY plan)
),
valid_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        plan,
        tier_value,
        tier_value_cume_dist,
        plan_stddev_samp_tier_value,
        plan_stddev_samp_tier_value_rounded,
        plan_avg_plan_stddev_samp_tier_value_rounded,
        plan_sum_plan_stddev_samp_tier_value,
        tier_value_filled,
        plan_sum_plan_stddev_samp_tier_value_num,
        plan_avg_plan_stddev_samp_tier_value_rounded_2,
        plan_count_tier_value_rank,
        tier_value_cume_dist / NULLIF(plan_stddev_samp_tier_value, 0) tier_value_cume_dist_per_plan_stddev_samp_tier_value
    FROM with_windows_price_tiers_2
    WHERE plan IN ('pro', 'enterprise')
)
SELECT
    plan,
    tier_value,
    tier_value_cume_dist,
    plan_stddev_samp_tier_value,
    plan_stddev_samp_tier_value_rounded,
    plan_sum_plan_stddev_samp_tier_value,
    tier_value_filled,
    plan_sum_plan_stddev_samp_tier_value_num,
    plan_avg_plan_stddev_samp_tier_value_rounded_2,
    CASE WHEN plan RLIKE '^(enterprise|starter)$' THEN 'OTHER_ZONE' ELSE 'EXTENDED' END AS plan_zone
FROM valid_price_tiers
ORDER BY plan_stddev_samp_tier_value DESC NULLS LAST
