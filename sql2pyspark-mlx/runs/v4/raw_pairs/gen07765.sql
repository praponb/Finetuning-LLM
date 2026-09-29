WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_prices) AS tier_value
    FROM price_tiers
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER w_running AS tier_value_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist,
        CUME_DIST() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist_2,
        STDDEV_SAMP(tier_value) OVER w_grp AS plan_stddev_samp_tier_value,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value NULLS FIRST) AS tier_value_cume_dist_3
    FROM exploded
    WINDOW w_running AS (ORDER BY tier_value NULLS FIRST), w_grp AS (PARTITION BY plan)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        ROUND(tier_value_rank / SUM(tier_value_rank) OVER (PARTITION BY plan), 4) AS tier_value_rank_share,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist_4
    FROM metrics
),
scored AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        tier_value,
        tier_value_rank,
        tier_value_cume_dist,
        tier_value_cume_dist_2,
        plan_stddev_samp_tier_value,
        tier_value_cume_dist_3,
        tier_value_rank_share,
        tier_value_cume_dist_4,
        MAX(plan_stddev_samp_tier_value) OVER (PARTITION BY plan) AS plan_max_plan_stddev_samp_tier_value,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist_3 DESC NULLS FIRST) AS tier_value_cume_dist_3_rank,
        DENSE_RANK() OVER (ORDER BY plan_stddev_samp_tier_value) AS plan_stddev_samp_tier_value_dense_rank,
        PERCENT_RANK() OVER (ORDER BY plan_stddev_samp_tier_value DESC) AS plan_stddev_samp_tier_value_percent_rank
    FROM enriched
),
scored_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY tier_value_cume_dist_4 NULLS FIRST) AS tier_value_cume_dist_4_dense_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_rank NULLS LAST) AS tier_value_rank_percent_rank,
        RANK() OVER (PARTITION BY plan ORDER BY plan_max_plan_stddev_samp_tier_value DESC NULLS FIRST) AS plan_max_plan_stddev_samp_tier_value_rank
    FROM scored
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        tier_value_rank,
        plan_stddev_samp_tier_value,
        tier_value_cume_dist_3,
        tier_value_rank_share,
        plan_max_plan_stddev_samp_tier_value,
        plan_stddev_samp_tier_value_dense_rank,
        plan_stddev_samp_tier_value_percent_rank,
        tier_value_rank_percent_rank,
        COALESCE(tier_value_cume_dist_2, 0) AS tier_value_cume_dist_2_filled,
        tier_value_rank_percent_rank / NULLIF(tier_value_cume_dist_4, 0) AS tier_value_rank_percent_rank_per_tier_value_cume_dist_4
    FROM scored_price_tiers
    WHERE effective_from > DATE '2024-04-27'
      AND plan_stddev_samp_tier_value_percent_rank < 0.23
),
report AS (
    SELECT
        id,
        plan,
        tier_value_rank,
        tier_value_rank_share,
        plan_max_plan_stddev_samp_tier_value,
        plan_stddev_samp_tier_value_dense_rank,
        tier_value_rank_percent_rank,
        tier_value_cume_dist_2_filled,
        tier_value_rank_percent_rank_per_tier_value_cume_dist_4,
        CASE WHEN plan RLIKE '^(starter|enterprise)$' THEN 'PRIORITY' ELSE 'EXTENDED' END AS plan_zone
    FROM cleaned
    ORDER BY tier_value_rank NULLS FIRST
)
SELECT
    id,
    plan,
    plan_max_plan_stddev_samp_tier_value,
    plan_stddev_samp_tier_value_dense_rank,
    tier_value_rank_percent_rank,
    tier_value_rank_percent_rank_per_tier_value_cume_dist_4
FROM report
ORDER BY tier_value_rank_percent_rank DESC, plan_max_plan_stddev_samp_tier_value
