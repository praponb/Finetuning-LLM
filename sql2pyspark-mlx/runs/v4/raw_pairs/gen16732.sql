WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE_OUTER(tier_limits) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_percent_rank,
        AVG(tier_value) OVER (PARTITION BY currency) AS currency_avg_tier_value
    FROM exploded
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        MIN(currency_avg_tier_value) OVER (PARTITION BY currency) AS currency_min_currency_avg_tier_value,
        COUNT(tier_value) OVER (PARTITION BY plan) AS plan_count_tier_value
    FROM metrics
),
with_windows_price_tiers AS (
    -- Ranking and gap features
    SELECT
        id,
        plan,
        currency,
        tier_value,
        tier_value_percent_rank,
        currency_avg_tier_value,
        currency_min_currency_avg_tier_value,
        plan_count_tier_value,
        ROUND(plan_count_tier_value / SUM(plan_count_tier_value) OVER (PARTITION BY plan), 4) AS plan_count_tier_value_share,
        SUM(currency_min_currency_avg_tier_value) OVER w_ordered AS currency_sum_currency_min_currency_avg_tier_value,
        AVG(tier_value) OVER w_ordered AS currency_avg_tier_value_2,
        AVG(currency_min_currency_avg_tier_value) OVER (PARTITION BY currency, plan) AS currency_avg_currency_min_currency_avg_tier_value
    FROM with_windows
    WINDOW w_ordered AS (PARTITION BY currency)
)
SELECT
    currency,
    currency_avg_tier_value,
    currency_min_currency_avg_tier_value,
    plan_count_tier_value,
    plan_count_tier_value_share,
    currency_sum_currency_min_currency_avg_tier_value,
    currency_avg_currency_min_currency_avg_tier_value,
    CASE WHEN currency RLIKE '^(THB|EUR)$' THEN 'EXTENDED' WHEN currency RLIKE '^(USD)$' THEN 'PRIORITY' ELSE 'CORE' END AS currency_zone
FROM with_windows_price_tiers
ORDER BY plan_count_tier_value_share ASC NULLS LAST, currency ASC;
