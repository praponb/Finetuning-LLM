WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS tier_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(tier_value) OVER (PARTITION BY currency, plan) AS currency_avg_tier_value,
        AVG(tier_value) OVER (PARTITION BY currency) AS currency_avg_tier_value_2
    FROM exploded
),
metrics AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        tier_value,
        currency_avg_tier_value,
        currency_avg_tier_value_2,
        RANK() OVER (PARTITION BY plan, currency ORDER BY currency_avg_tier_value DESC) AS currency_avg_tier_value_rank,
        RANK() OVER (PARTITION BY plan ORDER BY currency_avg_tier_value_2 NULLS LAST) AS currency_avg_tier_value_2_rank,
        MAX(tier_value) OVER (PARTITION BY currency) AS currency_max_tier_value,
        DENSE_RANK() OVER (PARTITION BY currency, plan ORDER BY currency_avg_tier_value NULLS LAST) AS currency_avg_tier_value_dense_rank,
        STDDEV_SAMP(currency_avg_tier_value) OVER (PARTITION BY plan) AS plan_stddev_samp_currency_avg_tier_value
    FROM scored
),
metrics_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        RANK() OVER w_part AS currency_avg_tier_value_rank_2,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY currency_avg_tier_value DESC NULLS FIRST) AS currency_avg_tier_value_percent_rank
    FROM metrics
    WINDOW w_part AS (ORDER BY currency_avg_tier_value DESC NULLS FIRST)
)
-- Native SQL PIVOT
SELECT *
FROM (
    SELECT
        currency,
        plan,
        currency_avg_tier_value_2
    FROM metrics_price_tiers
) src
PIVOT (
    MAX(currency_avg_tier_value_2) AS value_sum,
    COUNT(1) AS item_count
    FOR plan IN ('basic', 'pro')
);
