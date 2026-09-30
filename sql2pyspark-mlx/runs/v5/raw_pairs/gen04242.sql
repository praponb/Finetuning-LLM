WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) e AS tier_value
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, INITCAP(TRIM(currency)) AS currency_title
    FROM flattened
    WHERE currency IN ('THB', 'EUR', 'USD')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        currency,
        tier_value,
        currency_title,
        AVG(tier_value) OVER (PARTITION BY plan) AS plan_avg_tier_value,
        RANK() OVER w_running AS tier_value_rank,
        CUME_DIST() OVER w_running_2 AS tier_value_cume_dist,
        DENSE_RANK() OVER (PARTITION BY currency, plan ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY plan, currency) AS plan_stddev_samp_tier_value
    FROM cleaned
    WINDOW w_running AS (PARTITION BY plan, currency ORDER BY tier_value NULLS LAST), w_running_2 AS (PARTITION BY plan, currency ORDER BY tier_value DESC)
),
cleaned_price_tiers AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        plan_avg_tier_value,
        tier_value_rank,
        tier_value_dense_rank,
        plan_stddev_samp_tier_value,
        ROUND(plan_avg_tier_value * 1.07, 2) AS plan_avg_tier_value_scaled
    FROM metrics
    WHERE plan_stddev_samp_tier_value >= 43
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        tier_value_rank,
        tier_value_dense_rank,
        plan_stddev_samp_tier_value,
        plan_avg_tier_value_scaled,
        plan_avg_tier_value_scaled / NULLIF(plan_stddev_samp_tier_value, 0) AS plan_avg_tier_value_scaled_per_plan_stddev_samp_tier_value,
        plan_avg_tier_value_scaled - plan_stddev_samp_tier_value AS plan_avg_tier_value_scaled_minus_plan_stddev_samp_tier_value
    FROM cleaned_price_tiers
    WHERE plan_avg_tier_value_scaled <= 60
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        tier_value_rank,
        plan_stddev_samp_tier_value,
        SUM(tier_value_rank) OVER (PARTITION BY plan) AS plan_sum_tier_value_rank
    FROM filtered
    WHERE tier_value_rank <= 5
)
-- Multi-level aggregation
SELECT
    plan,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    COUNT(*) AS row_count,
    COUNT(*) AS cnt,
    SIZE(COLLECT_SET(plan)) AS num_plans_2
FROM with_windows
GROUP BY plan
