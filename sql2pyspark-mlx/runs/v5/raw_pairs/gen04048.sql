WITH lines AS (
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
metrics AS (
    SELECT
        plan,
        tier_value,
        DENSE_RANK() OVER (PARTITION BY plan, currency ORDER BY tier_value) AS tier_value_dense_rank,
        AVG(tier_value) OVER (PARTITION BY currency) AS currency_avg_tier_value,
        PERCENT_RANK() OVER (PARTITION BY plan, currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank,
        AVG(tier_value) OVER (PARTITION BY plan, currency) AS plan_avg_tier_value
    FROM lines
    WHERE currency IN ('EUR', 'USD')
),
scored AS (
    SELECT
        *,
        RANK() OVER (PARTITION BY plan ORDER BY currency_avg_tier_value DESC) AS currency_avg_tier_value_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_cume_dist,
        SUM(currency_avg_tier_value) OVER (PARTITION BY plan) AS plan_sum_currency_avg_tier_value,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_dense_rank_2
    FROM metrics
),
enriched AS (
    SELECT
        plan,
        tier_value,
        tier_value_dense_rank,
        currency_avg_tier_value,
        tier_value_percent_rank,
        plan_avg_tier_value,
        currency_avg_tier_value_rank,
        tier_value_cume_dist,
        plan_sum_currency_avg_tier_value,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY plan_sum_currency_avg_tier_value DESC) AS plan_sum_currency_avg_tier_value_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_avg_tier_value DESC NULLS LAST) AS plan_avg_tier_value_cume_dist
    FROM scored
)
SELECT
    plan,
    tier_value,
    tier_value_percent_rank,
    tier_value_cume_dist,
    plan_sum_currency_avg_tier_value,
    plan_avg_tier_value_cume_dist,
    STDDEV_SAMP(tier_value_cume_dist) OVER (PARTITION BY plan) AS plan_stddev_samp_tier_value_cume_dist
FROM enriched
