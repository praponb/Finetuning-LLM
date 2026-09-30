WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(tier_value) OVER (PARTITION BY currency, plan) AS currency_count_tier_value,
        RANK() OVER (ORDER BY tier_value ASC NULLS LAST) AS tier_value_rank,
        SUM(tier_value) OVER (PARTITION BY plan) AS plan_sum_tier_value
    FROM flattened
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        plan,
        effective_from,
        currency,
        tier_value,
        tier_value_rank,
        plan_sum_tier_value,
        DENSE_RANK() OVER (ORDER BY tier_value_rank DESC) AS tier_value_rank_dense_rank
    FROM scored
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        effective_from,
        plan_sum_tier_value,
        tier_value_rank_dense_rank,
        CEIL(tier_value_rank_dense_rank) AS tier_value_rank_dense_rank_whole,
        DAY(effective_from) AS effective_from_day
    FROM with_windows
    WHERE effective_from > TO_DATE('2024-02-17')
      AND currency IN ('USD', 'THB')
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(effective_from_day) OVER (PARTITION BY plan) AS plan_stddev_samp_effective_from_day,
        CUME_DIST() OVER (ORDER BY effective_from_day DESC) AS effective_from_day_cume_dist
    FROM cleaned
),
cleaned_price_tiers AS (
    -- Keep recent, valid rows
    SELECT *, CASE WHEN plan IN ('basic', 'enterprise') THEN 'Tier 1' ELSE 'Other' END AS plan_group
    FROM metrics
    WHERE effective_from < '2024-01-18'
),
stats AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('YEAR', effective_from) AS year_start,
        ROUND(AVG(plan_sum_tier_value), 2) AS avg_plan_sum_tier_value,
        SUM(CASE WHEN plan = 'starter' THEN plan_stddev_samp_effective_from_day ELSE 0 END) AS starter_plan_stddev_samp_effective_from_day,
        ROUND(SUM(tier_value_rank_dense_rank) / NULLIF(COUNT(CASE WHEN plan = 'basic' THEN 1 END), 0), 2) AS tier_value_rank_dense_rank_per_basic,
        SIZE(COLLECT_SET(plan)) AS num_plans
    FROM cleaned_price_tiers
    GROUP BY year_start
)
SELECT
    year_start,
    avg_plan_sum_tier_value,
    starter_plan_stddev_samp_effective_from_day,
    num_plans
FROM stats
