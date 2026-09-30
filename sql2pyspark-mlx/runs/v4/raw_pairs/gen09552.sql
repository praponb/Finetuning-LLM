WITH exploded AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_value
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) li AS tier_value
),
enriched AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_value,
        PERCENT_RANK() OVER (ORDER BY tier_value NULLS FIRST) AS tier_value_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY tier_value NULLS LAST) AS tier_value_cume_dist,
        PERCENT_RANK() OVER (ORDER BY tier_value DESC NULLS LAST) AS tier_value_percent_rank_2,
        CUME_DIST() OVER (ORDER BY tier_value DESC NULLS FIRST) AS tier_value_cume_dist_2
    FROM exploded
),
valid AS (
    SELECT
        *,
        ROUND(tier_value_percent_rank_2, 0) AS tier_value_percent_rank_2_rounded,
        tier_value_cume_dist_2 * tier_value_percent_rank_2 AS tier_value_cume_dist_2_times_tier_value_percent_rank_2
    FROM enriched
    WHERE effective_from > DATE '2024-10-25'
),
base AS (
    SELECT
        id,
        plan,
        effective_from,
        tier_value_percent_rank,
        tier_value_cume_dist,
        CAST(tier_value_percent_rank_2_rounded AS INT) AS tier_value_percent_rank_2_rounded_int
    FROM valid
    WHERE tier_value_percent_rank BETWEEN 0.23 AND 0.28
)
SELECT
    plan,
    tier_value_cume_dist,
    CASE WHEN plan RLIKE '^(enterprise|pro)$' THEN 'PRIORITY' WHEN plan RLIKE '^(starter|basic)$' THEN 'CORE' ELSE 'EXTENDED' END AS plan_zone
FROM base
