WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS tier_value
    FROM price_tiers
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER w_grp AS tier_value_rank,
        RANK() OVER (ORDER BY tier_value ASC NULLS FIRST) AS tier_value_rank_2,
        PERCENT_RANK() OVER (PARTITION BY currency ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank
    FROM lines
    WINDOW w_grp AS (PARTITION BY currency ORDER BY tier_value DESC NULLS FIRST)
),
filtered AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN tier_value_percent_rank >= 0.86 THEN 'High' WHEN tier_value_percent_rank >= 0.39 THEN 'Medium' ELSE 'Low' END AS tier_value_percent_rank_level
    FROM enriched
    WHERE effective_from >= DATE '2024-03-23'
),
enriched_price_tiers AS (
    -- Ranking and gap features
    SELECT
        plan,
        tier_value,
        tier_value_percent_rank,
        tier_value_percent_rank_level,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value_rank DESC) AS tier_value_rank_cume_dist,
        MIN(tier_value_percent_rank) OVER (PARTITION BY plan) AS plan_min_tier_value_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value_rank_2 ASC NULLS LAST) AS tier_value_rank_2_cume_dist,
        PERCENT_RANK() OVER (PARTITION BY currency, plan ORDER BY tier_value DESC NULLS FIRST) AS tier_value_percent_rank_2,
        CUME_DIST() OVER (ORDER BY tier_value_percent_rank ASC NULLS LAST) AS tier_value_percent_rank_cume_dist
    FROM filtered
)
-- Aggregate with conditional metrics
SELECT
    CASE WHEN tier_value >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END AS tier_value_tier,
    MIN(tier_value_percent_rank) AS min_tier_value_percent_rank,
    PERCENTILE_APPROX(tier_value_rank_cume_dist, 0.95) AS tier_value_rank_cume_dist_p95,
    ROUND(AVG(tier_value_rank_2_cume_dist), 2) AS avg_tier_value_rank_2_cume_dist,
    APPROX_PERCENTILE(tier_value_rank_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS tier_value_rank_cume_dist_quartiles,
    SIZE(COLLECT_SET(plan)) AS num_plans
FROM enriched_price_tiers
GROUP BY CASE WHEN tier_value >= 85 THEN 'PREMIUM' ELSE 'STANDARD' END
HAVING SIZE(COLLECT_SET(plan)) < 1000
ORDER BY tier_value_tier ASC;
