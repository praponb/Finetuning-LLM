WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, COALESCE(tier_value, 0) AS tier_value_filled
    FROM exploded
    WHERE effective_from > DATE '2024-01-23'
),
valid_price_tiers AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(tier_value_filled, 2) AS tier_value_filled_rounded
    FROM valid
    WHERE effective_from >= DATE '2024-05-15'
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        AVG(tier_value_filled_rounded) OVER (PARTITION BY currency) AS currency_avg_tier_value_filled_rounded,
        RANK() OVER w_grp AS tier_value_filled_rounded_rank,
        CUME_DIST() OVER (ORDER BY tier_value_filled_rounded NULLS LAST) AS tier_value_filled_rounded_cume_dist
    FROM valid_price_tiers
    WHERE currency IN ('USD', 'THB', 'EUR')
    WINDOW w_grp AS (PARTITION BY plan, currency ORDER BY tier_value_filled_rounded DESC NULLS LAST)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        id,
        plan,
        effective_from,
        currency,
        tier_value,
        tier_value_filled_rounded,
        tier_value_filled_rounded_rank,
        tier_value_filled_rounded_cume_dist,
        SUBSTRING(plan, 1, 3) AS plan_prefix,
        ROUND(tier_value_filled_rounded_cume_dist, 0) AS tier_value_filled_rounded_cume_dist_rounded
    FROM scored
    WHERE effective_from >= TO_DATE('2024-04-26')
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY tier_value_filled_rounded NULLS FIRST) AS tier_value_filled_rounded_cume_dist_2,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value_filled_rounded_rank NULLS LAST) AS tier_value_filled_rounded_rank_rank,
        STDDEV_SAMP(tier_value) OVER w_running AS plan_stddev_samp_tier_value,
        STDDEV_SAMP(tier_value_filled_rounded_cume_dist) OVER w_running AS plan_stddev_samp_tier_value_filled_rounded_cume_dist
    FROM filtered
    WINDOW w_running AS (PARTITION BY plan)
)
-- Multi-level aggregation
SELECT
    CASE WHEN plan_stddev_samp_tier_value >= 82 THEN 'HIGH' ELSE 'LOW' END AS plan_stddev_samp_tier_value_tier,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    ROUND(SUM(plan_stddev_samp_tier_value) / NULLIF(COUNT(CASE WHEN currency = 'EUR' THEN 1 END), 0), 2) AS plan_stddev_samp_tier_value_per_eur,
    SIZE(COLLECT_SET(currency)) AS num_currencies,
    AVG(tier_value_filled_rounded_rank) AS avg_tier_value_filled_rounded_rank
FROM scored_price_tiers
GROUP BY CASE WHEN plan_stddev_samp_tier_value >= 82 THEN 'HIGH' ELSE 'LOW' END, DATE_TRUNC('MONTH', effective_from);
