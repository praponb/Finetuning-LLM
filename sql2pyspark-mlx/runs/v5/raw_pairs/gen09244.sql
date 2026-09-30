WITH exploded AS (
    SELECT
        id,
        plan,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS tier_value
),
filtered AS (
    SELECT *, CASE WHEN plan = 'enterprise' THEN 1 ELSE 0 END AS is_enterprise
    FROM exploded
    WHERE plan = 'starter'
),
metrics AS (
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value DESC) AS tier_value_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY is_enterprise ASC) AS is_enterprise_rank,
        MAX(tier_value) OVER w_part AS plan_max_tier_value,
        MIN(tier_value) OVER w_part AS plan_min_tier_value,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value ASC) AS tier_value_cume_dist_2
    FROM filtered
    WHERE tier_value BETWEEN 61 AND 65
    WINDOW w_part AS (PARTITION BY plan)
),
enriched AS (
    SELECT *, STDDEV_SAMP(plan_min_tier_value) OVER (PARTITION BY plan) AS plan_stddev_samp_plan_min_tier_value
    FROM metrics
),
enriched_price_tiers AS (
    SELECT *, RANK() OVER (PARTITION BY plan ORDER BY is_enterprise DESC NULLS LAST) AS is_enterprise_rank_2
    FROM enriched
),
scored AS (
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY is_enterprise_rank DESC NULLS LAST) AS is_enterprise_rank_percent_rank,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist_2 DESC NULLS FIRST) AS tier_value_cume_dist_2_rank,
        RANK() OVER (PARTITION BY plan ORDER BY tier_value_cume_dist_2 DESC NULLS LAST) AS tier_value_cume_dist_2_rank_2
    FROM enriched_price_tiers
),
kept_keys AS (
    SELECT plan, id
    FROM scored
    INTERSECT
    SELECT plan, id
    FROM price_tiers
    WHERE plan <> 'enterprise'
),
kept_rows AS (
    SELECT s.*
    FROM scored s
    INNER JOIN kept_keys k ON s.plan = k.plan AND s.id = k.id
)
SELECT
    CASE WHEN plan_min_tier_value >= 68 THEN 'HIGH' ELSE 'LOW' END AS plan_min_tier_value_tier,
    COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count,
    COUNT(DISTINCT plan) AS distinct_plans,
    SIZE(COLLECT_SET(plan)) AS num_plans,
    GROUPING_ID() AS grouping_level
FROM kept_rows
GROUP BY ROLLUP (CASE WHEN plan_min_tier_value >= 68 THEN 'HIGH' ELSE 'LOW' END)
ORDER BY distinct_plans DESC;
