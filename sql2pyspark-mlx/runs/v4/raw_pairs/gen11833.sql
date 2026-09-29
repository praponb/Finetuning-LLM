WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) t AS tier_value
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value ASC NULLS LAST) AS tier_value_cume_dist,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_cume_dist_2
    FROM exploded
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        tier_value,
        tier_value_cume_dist,
        tier_value_cume_dist_2,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist ASC NULLS LAST) AS tier_value_cume_dist_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist_2 ASC NULLS FIRST) AS tier_value_cume_dist_2_rank
    FROM scored
),
enriched_price_tiers AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist_2 DESC) AS tier_value_cume_dist_2_cume_dist,
        DENSE_RANK() OVER w_running AS tier_value_cume_dist_2_rank_dense_rank,
        PERCENT_RANK() OVER (ORDER BY tier_value_cume_dist_2_rank DESC) AS tier_value_cume_dist_2_rank_percent_rank
    FROM enriched
    WINDOW w_running AS (PARTITION BY plan ORDER BY tier_value_cume_dist_2_rank ASC)
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(tier_value) OVER w_running AS plan_avg_tier_value,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist_2 DESC) AS tier_value_cume_dist_2_dense_rank,
        RANK() OVER (ORDER BY tier_value_cume_dist_2_rank_percent_rank ASC) AS tier_value_cume_dist_2_rank_percent_rank_rank,
        RANK() OVER w_grp AS tier_value_cume_dist_2_rank_2,
        SUM(tier_value_cume_dist_2_cume_dist) OVER w_running AS plan_sum_tier_value_cume_dist_2_cume_dist
    FROM enriched_price_tiers
    WINDOW w_running AS (PARTITION BY plan), w_grp AS (ORDER BY tier_value_cume_dist_2 DESC NULLS FIRST)
)
SELECT
    id,
    plan,
    tier_value_cume_dist,
    tier_value_cume_dist_2,
    tier_value_cume_dist_cume_dist,
    tier_value_cume_dist_2_cume_dist,
    tier_value_cume_dist_2_rank_dense_rank,
    tier_value_cume_dist_2_rank_percent_rank,
    tier_value_cume_dist_2_dense_rank,
    CASE WHEN plan RLIKE '^(starter|enterprise)$' THEN 'PRIORITY' WHEN plan RLIKE '^(basic|pro)$' THEN 'GROUP_A' ELSE 'EXTENDED' END AS plan_zone
FROM with_windows;
