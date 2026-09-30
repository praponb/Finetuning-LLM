WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_limits) t AS tier_value
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(tier_value) OVER (PARTITION BY plan, currency) AS plan_stddev_samp_tier_value,
        PERCENT_RANK() OVER (ORDER BY tier_value ASC) AS tier_value_percent_rank
    FROM flattened
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        currency,
        tier_value,
        plan_stddev_samp_tier_value,
        tier_value_percent_rank,
        UPPER(plan) AS plan_upper
    FROM enriched
    WHERE plan IN ('enterprise', 'starter', 'pro')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_ordered AS tier_value_percent_rank_cume_dist,
        COUNT(tier_value) OVER (PARTITION BY currency, plan) AS currency_count_tier_value,
        DENSE_RANK() OVER (ORDER BY plan_stddev_samp_tier_value DESC) AS plan_stddev_samp_tier_value_dense_rank
    FROM valid
    WINDOW w_ordered AS (PARTITION BY plan, currency ORDER BY tier_value_percent_rank DESC NULLS LAST)
)
SELECT
    id,
    plan,
    currency,
    tier_value,
    plan_stddev_samp_tier_value,
    tier_value_percent_rank,
    plan_upper,
    tier_value_percent_rank_cume_dist,
    currency_count_tier_value
FROM metrics
ORDER BY tier_value_percent_rank_cume_dist DESC;
