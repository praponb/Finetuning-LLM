WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(tier_value, 0) AS tier_value_rounded
    FROM lines
    WHERE effective_from > TO_DATE('2024-01-24')
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        currency,
        tier_value,
        tier_value_rounded,
        SUM(tier_value) OVER (PARTITION BY plan) AS plan_sum_tier_value,
        AVG(tier_value_rounded) OVER (PARTITION BY plan) AS plan_avg_tier_value_rounded,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_cume_dist,
        MAX(tier_value) OVER (PARTITION BY currency) AS currency_max_tier_value,
        CUME_DIST() OVER (PARTITION BY plan, currency ORDER BY tier_value_rounded DESC NULLS FIRST) AS tier_value_rounded_cume_dist
    FROM cleaned
    WHERE effective_from <= '2024-03-01'
),
metrics AS (
    -- Ranking and gap features
    SELECT
        tier_value,
        plan_avg_tier_value_rounded,
        tier_value_cume_dist,
        COUNT(tier_value) OVER (PARTITION BY currency) AS currency_count_tier_value,
        AVG(tier_value_rounded) OVER (PARTITION BY currency) AS currency_avg_tier_value_rounded
    FROM scored
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_percent_rank,
        RANK() OVER (ORDER BY tier_value_cume_dist NULLS FIRST) AS tier_value_cume_dist_rank
    FROM metrics
),
valid AS (
    -- Keep recent, valid rows
    SELECT
        tier_value,
        plan_avg_tier_value_rounded,
        currency_count_tier_value,
        currency_avg_tier_value_rounded,
        tier_value_percent_rank,
        tier_value_cume_dist_rank,
        plan_avg_tier_value_rounded + tier_value_cume_dist AS plan_avg_tier_value_rounded_plus_tier_value_cume_dist,
        ROUND(tier_value_cume_dist_rank, 0) AS tier_value_cume_dist_rank_rounded
    FROM with_windows
    WHERE tier_value_cume_dist_rank BETWEEN 5 AND 7
),
cleaned_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        tier_value,
        plan_avg_tier_value_rounded,
        currency_avg_tier_value_rounded,
        tier_value_percent_rank,
        tier_value_cume_dist_rank,
        plan_avg_tier_value_rounded_plus_tier_value_cume_dist,
        tier_value_cume_dist_rank_rounded,
        CAST(tier_value_cume_dist_rank AS DOUBLE) AS tier_value_cume_dist_rank_num
    FROM valid
    WHERE tier_value > 27
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tier_value_cume_dist_rank_rounded >= 62 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_cume_dist_rank_rounded_tier,
    CASE WHEN tier_value >= 72 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_tier,
    COUNT(*) AS event_count,
    ROUND(AVG(plan_avg_tier_value_rounded_plus_tier_value_cume_dist), 2) AS avg_plan_avg_tier_value_rounded_plus_tier_value_cume_dist,
    APPROX_PERCENTILE(tier_value_cume_dist_rank, ARRAY(0.25, 0.5, 0.75)) AS tier_value_cume_dist_rank_quartiles
FROM cleaned_price_tiers
GROUP BY 1, 2
ORDER BY avg_plan_avg_tier_value_rounded_plus_tier_value_cume_dist DESC;
