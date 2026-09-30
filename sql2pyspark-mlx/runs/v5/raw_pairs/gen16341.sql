WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) e AS tier_value
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        tier_value,
        MIN(tier_value) OVER (PARTITION BY plan) AS plan_min_tier_value,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist,
        SUM(tier_value) OVER (PARTITION BY plan) AS plan_sum_tier_value,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value ASC NULLS LAST) AS tier_value_rank
    FROM flattened
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(tier_value_rank / SUM(tier_value_rank) OVER (PARTITION BY plan), 4) AS tier_value_rank_share,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_sum_tier_value ASC NULLS FIRST) AS plan_sum_tier_value_cume_dist
    FROM metrics
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS LAST) AS tier_value_cume_dist_2,
        DENSE_RANK() OVER w_grp AS tier_value_rank_share_dense_rank,
        CUME_DIST() OVER (ORDER BY plan_sum_tier_value DESC) AS plan_sum_tier_value_cume_dist_2,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value_rank_share DESC NULLS FIRST) AS tier_value_rank_share_dense_rank_2
    FROM with_windows
    WINDOW w_grp AS (PARTITION BY plan ORDER BY tier_value_rank_share DESC)
),
scored_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist DESC) AS tier_value_cume_dist_percent_rank,
        COUNT(tier_value_cume_dist) OVER (PARTITION BY plan) AS plan_count_tier_value_cume_dist
    FROM scored
    WHERE tier_value_cume_dist_2 >= 0.73
),
summary AS (
    -- Grouped metrics
    SELECT
        DATE_TRUNC('YEAR', effective_from) AS year_start,
        CASE WHEN tier_value_cume_dist_2 >= 0.76 THEN 'LARGE' ELSE 'SMALL' END AS tier_value_cume_dist_2_tier,
        COUNT_IF(plan = 'basic') AS basic_events,
        SUM(CASE WHEN plan = 'enterprise' THEN tier_value_cume_dist ELSE 0 END) AS enterprise_tier_value_cume_dist,
        COUNT_IF(plan = 'starter') AS starter_events,
        PERCENT_RANK() OVER (PARTITION BY DATE_TRUNC('YEAR', effective_from) ORDER BY SUM(CASE WHEN plan = 'enterprise' THEN tier_value_cume_dist ELSE 0 END) DESC) AS enterprise_tier_value_cume_dist_percent_rank
    FROM scored_price_tiers
    GROUP BY year_start, tier_value_cume_dist_2_tier
)
SELECT
    year_start,
    tier_value_cume_dist_2_tier,
    basic_events,
    enterprise_tier_value_cume_dist,
    starter_events,
    enterprise_tier_value_cume_dist_percent_rank
FROM summary
ORDER BY tier_value_cume_dist_2_tier ASC NULLS LAST;
