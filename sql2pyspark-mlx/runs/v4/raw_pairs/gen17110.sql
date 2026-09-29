WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS val
),
base AS (
    -- Keep recent, valid rows
    SELECT *, DATEDIFF(CURRENT_DATE(), effective_from) AS days_since_effective_from
    FROM exploded
    WHERE effective_from > '2023-08-01'
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY val ASC NULLS LAST) AS val_cume_dist,
        MAX(val) OVER w_part AS plan_max_val,
        COUNT(days_since_effective_from) OVER w_part AS plan_count_days_since_effective_from,
        RANK() OVER w_hist AS val_rank
    FROM base
    WINDOW w_part AS (PARTITION BY plan), w_hist AS (PARTITION BY plan ORDER BY val ASC NULLS FIRST)
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        CUME_DIST() OVER w_recent AS plan_count_days_since_effective_from_cume_dist,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val_cume_dist DESC NULLS LAST) AS val_cume_dist_dense_rank,
        ROUND(plan_count_days_since_effective_from / SUM(plan_count_days_since_effective_from) OVER (PARTITION BY plan), 4) AS plan_count_days_since_effective_from_share,
        ROUND(plan_count_days_since_effective_from / SUM(plan_count_days_since_effective_from) OVER (PARTITION BY plan), 4) AS plan_count_days_since_effective_from_share_2,
        MAX(val_rank) OVER w_running AS plan_max_val_rank
    FROM scored
    WINDOW w_recent AS (PARTITION BY plan ORDER BY plan_count_days_since_effective_from ASC NULLS FIRST), w_running AS (PARTITION BY plan)
),
grouped AS (
    -- Grouped metrics
    SELECT
        plan,
        CASE WHEN plan_count_days_since_effective_from >= 17 THEN 'LARGE' ELSE 'SMALL' END AS plan_count_days_since_effective_from_tier,
        SIZE(COLLECT_SET(plan)) AS num_plans,
        PERCENTILE_APPROX(val_cume_dist_dense_rank, 0.5) AS val_cume_dist_dense_rank_p50,
        ROUND(SUM(plan_count_days_since_effective_from_share_2) / NULLIF(COUNT(CASE WHEN plan = 'enterprise' THEN 1 END), 0), 2) AS plan_count_days_since_effective_from_share_2_per_enterprise,
        SIZE(COLLECT_SET(plan)) AS num_plans_2,
        COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count
    FROM metrics
    GROUP BY plan, CASE WHEN plan_count_days_since_effective_from >= 17 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    plan,
    plan_count_days_since_effective_from_tier,
    val_cume_dist_dense_rank_p50,
    num_plans_2
FROM grouped;
