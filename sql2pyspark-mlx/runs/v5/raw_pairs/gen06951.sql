WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
),
valid AS (
    -- Basic cleansing and defaults
    SELECT *, CASE WHEN val >= 67 THEN 'High' WHEN val >= 31 THEN 'Medium' ELSE 'Low' END AS val_tier
    FROM exploded
    WHERE effective_from > DATE '2024-04-17'
),
base AS (
    -- Keep recent, valid rows
    SELECT
        plan,
        effective_from,
        val_tier,
        ROUND(val * 1.07, 2) AS val_scaled,
        CEIL(val) AS val_whole
    FROM valid
    WHERE effective_from >= DATE '2024-05-31'
      AND effective_from BETWEEN '2023-08-07' AND '2025-05-01'
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        effective_from,
        val_scaled,
        val_whole,
        DENSE_RANK() OVER (ORDER BY val_scaled DESC) AS val_scaled_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY val_scaled DESC NULLS FIRST) AS val_scaled_cume_dist
    FROM base
),
summary AS (
    -- Aggregate with conditional metrics
    SELECT
        CASE WHEN val_whole >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END AS val_whole_tier,
        CASE WHEN val_scaled >= 75 THEN 'LARGE' ELSE 'SMALL' END AS val_scaled_tier,
        APPROX_PERCENTILE(val_whole, 0.95) AS val_whole_p95,
        APPROX_PERCENTILE(val_scaled_cume_dist, ARRAY(0.25, 0.5, 0.75)) AS val_scaled_cume_dist_quartiles,
        MAX(val_whole) AS max_val_whole
    FROM with_windows
    GROUP BY CASE WHEN val_whole >= 88 THEN 'PREMIUM' ELSE 'STANDARD' END, CASE WHEN val_scaled >= 75 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    val_whole_tier,
    val_scaled_tier,
    val_scaled_cume_dist_quartiles,
    max_val_whole
FROM summary
ORDER BY val_whole_tier NULLS LAST, val_scaled_tier DESC;
