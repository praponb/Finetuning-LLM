WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) x AS tier_value
),
base AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        TRIM(plan) AS clean_plan,
        UPPER(currency) AS currency_upper
    FROM lines
    WHERE currency IN ('THB', 'USD')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        effective_from,
        currency,
        currency_upper,
        CUME_DIST() OVER (ORDER BY tier_value DESC) AS tier_value_cume_dist,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY tier_value DESC NULLS FIRST) AS tier_value_dense_rank,
        CUME_DIST() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist_3,
        SUM(tier_value) OVER (PARTITION BY currency) AS currency_sum_tier_value
    FROM base
    WHERE plan != 'starter'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        STDDEV_SAMP(currency_sum_tier_value) OVER (PARTITION BY plan, currency) AS plan_stddev_samp_currency_sum_tier_value,
        COUNT(tier_value_cume_dist_3) OVER (PARTITION BY plan) AS plan_count_tier_value_cume_dist_3,
        MAX(tier_value_cume_dist_3) OVER (PARTITION BY currency) AS currency_max_tier_value_cume_dist_3
    FROM scored
)
SELECT
    currency,
    currency_upper,
    tier_value_cume_dist,
    tier_value_cume_dist_2,
    tier_value_dense_rank,
    tier_value_cume_dist_3,
    plan_stddev_samp_currency_sum_tier_value,
    currency_max_tier_value_cume_dist_3,
    CASE WHEN currency RLIKE '^(THB|EUR)$' THEN 'CORE' ELSE 'GROUP_B' END AS currency_zone
FROM metrics
ORDER BY currency DESC NULLS LAST, currency_max_tier_value_cume_dist_3 DESC;
