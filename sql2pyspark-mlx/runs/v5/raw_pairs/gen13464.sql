WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) li AS val
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        currency,
        RANK() OVER (ORDER BY val DESC NULLS FIRST) val_rank
    FROM exploded
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        plan,
        effective_from,
        currency,
        val_rank,
        CASE WHEN plan = 'pro' THEN 1 ELSE 0 END is_pro
    FROM metrics
    WHERE plan = 'pro'
),
scored AS (
    -- Running and moving aggregates
    SELECT
        effective_from,
        val_rank,
        ROUND(val_rank / SUM(val_rank) OVER (PARTITION BY plan), 4) val_rank_share,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY val_rank NULLS LAST) val_rank_cume_dist,
        MAX(is_pro) OVER (PARTITION BY plan) plan_max_is_pro
    FROM valid
)
-- Multi-level aggregation
SELECT
    CASE WHEN val_rank_share >= 0.72 THEN 'LARGE' ELSE 'SMALL' END AS val_rank_share_tier,
    CASE WHEN val_rank_cume_dist >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_rank_cume_dist_tier,
    DATE_TRUNC('MONTH', effective_from) AS activity_month,
    PERCENTILE_APPROX(val_rank, ARRAY(0.25, 0.5, 0.75)) AS val_rank_quartiles,
    COUNT(*) AS cnt,
    COUNT(*) AS cnt_2,
    ROUND(AVG(val_rank_share), 2) AS avg_val_rank_share,
    ROUND(AVG(val_rank), 2) AS avg_val_rank,
    PERCENTILE_APPROX(val_rank_cume_dist, 0.9) AS val_rank_cume_dist_p90
FROM scored
GROUP BY ROLLUP (CASE WHEN val_rank_share >= 0.72 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN val_rank_cume_dist >= 0.63 THEN 'PREMIUM' ELSE 'STANDARD' END, DATE_TRUNC('MONTH', effective_from))
HAVING val_rank_cume_dist_p90 >= 0
ORDER BY val_rank_share_tier DESC NULLS LAST;
