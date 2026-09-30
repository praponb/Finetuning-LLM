WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE(discounts) AS val
    FROM price_tiers
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(val, 2) AS val_rounded
    FROM exploded
    WHERE plan IN ('pro', 'starter', 'enterprise')
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY val DESC) AS val_cume_dist,
        SUM(val) OVER (PARTITION BY plan) AS plan_sum_val,
        RANK() OVER (PARTITION BY plan ORDER BY val_rounded NULLS FIRST) AS val_rounded_rank
    FROM base
),
grouped AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN val_cume_dist >= 0.62 THEN 'HIGH' ELSE 'LOW' END AS val_cume_dist_tier,
        CASE WHEN val >= 70 THEN 'LARGE' ELSE 'SMALL' END AS val_tier,
        COUNT_IF(plan = 'starter') AS starter_events,
        ROUND(AVG(val_rounded), 2) AS avg_val_rounded,
        DENSE_RANK() OVER (PARTITION BY CASE WHEN val_cume_dist >= 0.62 THEN 'HIGH' ELSE 'LOW' END ORDER BY COUNT_IF(plan = 'starter') DESC) AS starter_events_dense_rank
    FROM with_windows
    GROUP BY CASE WHEN val_cume_dist >= 0.62 THEN 'HIGH' ELSE 'LOW' END, CASE WHEN val >= 70 THEN 'LARGE' ELSE 'SMALL' END
)
SELECT
    val_cume_dist_tier,
    val_tier,
    starter_events_dense_rank
FROM grouped
ORDER BY val_cume_dist_tier DESC;
