WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_prices) li AS tier_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        MAX(tier_value) OVER (PARTITION BY plan) AS plan_max_tier_value,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist,
        COUNT(tier_value) OVER (PARTITION BY plan) AS plan_count_tier_value,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY tier_value) AS tier_value_cume_dist_2,
        COUNT(tier_value) OVER (PARTITION BY currency) AS currency_count_tier_value
    FROM lines
),
filtered AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        CASE WHEN currency_count_tier_value >= 13 THEN 'High' WHEN currency_count_tier_value >= 7 THEN 'Medium' ELSE 'Low' END AS currency_count_tier_value_band,
        tier_value_cume_dist_2 + currency_count_tier_value AS tier_value_cume_dist_2_plus_currency_count_tier_value
    FROM scored
    WHERE currency_count_tier_value <= 12
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        tier_value_cume_dist,
        tier_value_cume_dist_2,
        currency_count_tier_value,
        LEAST(plan_count_tier_value, tier_value_cume_dist_2_plus_currency_count_tier_value) AS least_plan_count_tier_value_tier_value_cume_dist_2_plus_currency_count_tier_value
    FROM filtered
    WHERE effective_from > DATE '2024-04-30'
      AND plan = 'pro'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value_cume_dist_2,
        currency_count_tier_value,
        least_plan_count_tier_value_tier_value_cume_dist_2_plus_currency_count_tier_value,
        tier_value_cume_dist_2 * currency_count_tier_value AS tier_value_cume_dist_2_times_currency_count_tier_value,
        ROUND(least_plan_count_tier_value_tier_value_cume_dist_2_plus_currency_count_tier_value, 0) AS least_plan_count_tier_value_tier_value_cume_dist_2_plus_currency_count_tier_value_rounded
    FROM valid
    WHERE effective_from >= DATE '2024-03-08'
)
SELECT
    id,
    plan,
    currency,
    tier_value_cume_dist_2,
    currency_count_tier_value,
    least_plan_count_tier_value_tier_value_cume_dist_2_plus_currency_count_tier_value,
    tier_value_cume_dist_2_times_currency_count_tier_value
FROM base
ORDER BY currency DESC NULLS LAST, plan DESC
