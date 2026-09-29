WITH lines AS (
    SELECT
        id,
        plan,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS val
),
enriched AS (
    SELECT
        *,
        AVG(val) OVER (PARTITION BY plan) plan_avg_val,
        RANK() OVER (PARTITION BY plan ORDER BY val NULLS LAST) val_rank,
        CUME_DIST() OVER (ORDER BY val NULLS FIRST) val_cume_dist,
        RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) val_rank_2,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val) val_dense_rank
    FROM lines
),
base AS (
    SELECT *, val_rank - val_dense_rank val_rank_minus_val_dense_rank
    FROM enriched
    WHERE plan IN ('enterprise', 'starter', 'pro')
),
metrics AS (
    SELECT
        id,
        plan,
        val,
        plan_avg_val,
        val_rank,
        val_cume_dist,
        val_rank_2,
        val_dense_rank,
        val_rank_minus_val_dense_rank,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val_cume_dist NULLS FIRST) val_cume_dist_dense_rank,
        RANK() OVER (ORDER BY plan_avg_val DESC) plan_avg_val_rank
    FROM base
)
SELECT
    plan,
    COUNT(DISTINCT plan) AS distinct_plans,
    ROUND(SUM(val_cume_dist_dense_rank) / NULLIF(COUNT(CASE WHEN plan = 'pro' THEN 1 END), 0), 2) AS val_cume_dist_dense_rank_per_pro
FROM metrics
GROUP BY plan
ORDER BY plan DESC;
