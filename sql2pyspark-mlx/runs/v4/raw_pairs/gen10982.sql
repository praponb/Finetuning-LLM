WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT plan, currency
    FROM lines
    INTERSECT
    SELECT plan, currency
    FROM price_tiers
    WHERE effective_from BETWEEN '2023-07-01' AND '2025-01-12'
),
verified AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    INNER JOIN remaining_keys k ON s.plan = k.plan AND s.currency = k.currency
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(tier_value) OVER w_recent AS currency_max_tier_value,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value ASC NULLS LAST) AS tier_value_rank,
        AVG(tier_value) OVER w_recent AS currency_avg_tier_value,
        SUM(tier_value) OVER (PARTITION BY plan) AS plan_sum_tier_value,
        CUME_DIST() OVER w_hist AS tier_value_cume_dist
    FROM verified
    WINDOW w_recent AS (PARTITION BY currency), w_hist AS (PARTITION BY plan, currency ORDER BY tier_value DESC)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        currency,
        tier_value,
        currency_max_tier_value,
        tier_value_rank,
        currency_avg_tier_value,
        plan_sum_tier_value,
        tier_value_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist DESC NULLS FIRST) AS tier_value_cume_dist_cume_dist,
        SUM(plan_sum_tier_value) OVER (PARTITION BY currency) AS currency_sum_plan_sum_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank
    FROM enriched
),
enriched_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(tier_value_rank / SUM(tier_value_rank) OVER (PARTITION BY currency), 4) AS tier_value_rank_share,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist_cume_dist DESC NULLS LAST) AS tier_value_cume_dist_cume_dist_dense_rank
    FROM with_windows
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        tier_value,
        currency_max_tier_value,
        tier_value_rank,
        currency_avg_tier_value,
        plan_sum_tier_value,
        tier_value_cume_dist,
        tier_value_cume_dist_cume_dist,
        currency_sum_plan_sum_tier_value,
        tier_value_dense_rank,
        tier_value_rank_share,
        tier_value_cume_dist_cume_dist_dense_rank,
        MIN(currency_sum_plan_sum_tier_value) OVER (PARTITION BY plan) AS plan_min_currency_sum_plan_sum_tier_value,
        ROUND(tier_value_dense_rank / SUM(tier_value_dense_rank) OVER (PARTITION BY currency), 4) AS tier_value_dense_rank_share,
        PERCENT_RANK() OVER w_recent AS plan_sum_tier_value_percent_rank,
        STDDEV_SAMP(plan_sum_tier_value) OVER w_hist AS plan_stddev_samp_plan_sum_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY tier_value_cume_dist_cume_dist DESC NULLS LAST) AS tier_value_cume_dist_cume_dist_dense_rank_2
    FROM enriched_price_tiers
    WINDOW w_recent AS (ORDER BY plan_sum_tier_value DESC), w_hist AS (PARTITION BY plan, currency)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, FLOOR(tier_value) AS tier_value_whole
    FROM metrics
    WHERE tier_value_dense_rank_share BETWEEN 0.68 AND 0.72
)
SELECT
    plan,
    tier_value,
    tier_value_rank,
    plan_sum_tier_value,
    tier_value_cume_dist,
    tier_value_cume_dist_cume_dist,
    currency_sum_plan_sum_tier_value,
    tier_value_dense_rank,
    tier_value_rank_share,
    CASE WHEN plan RLIKE '^(pro|starter)$' THEN 'GROUP_A' ELSE 'OTHER_ZONE' END AS plan_zone
FROM base
ORDER BY tier_value_cume_dist_cume_dist ASC, tier_value_rank_share DESC;
