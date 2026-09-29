WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        EXPLODE_OUTER(tier_limits) AS val
    FROM price_tiers
),
with_windows AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val,
        MAX(val) OVER (PARTITION BY plan) AS plan_max_val,
        SUM(val) OVER (PARTITION BY plan) AS plan_sum_val,
        CUME_DIST() OVER (ORDER BY val DESC) AS val_cume_dist,
        CUME_DIST() OVER (ORDER BY val DESC NULLS LAST) AS val_cume_dist_2
    FROM lines
),
remaining_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM with_windows
    EXCEPT ALL
    SELECT id, plan
    FROM price_tiers
    WHERE currency IN ('EUR', 'USD', 'THB')
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows s
    INNER JOIN remaining_keys k ON s.id = k.id AND s.plan = k.plan
),
with_windows_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY plan_max_val DESC) AS plan_max_val_dense_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_sum_val DESC) AS plan_sum_val_cume_dist,
        ROUND(plan_count_val / SUM(plan_count_val) OVER (PARTITION BY plan), 4) AS plan_count_val_share
    FROM kept_rows
),
matched_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM with_windows_price_tiers
    EXCEPT DISTINCT
    SELECT id, plan
    FROM price_tiers
    WHERE currency IN ('EUR', 'THB', 'USD')
),
kept_rows_price_tiers AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM with_windows_price_tiers s
    INNER JOIN matched_keys k ON s.id = k.id AND s.plan = k.plan
),
with_windows_price_tiers_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY plan_max_val_dense_rank DESC NULLS LAST) AS plan_max_val_dense_rank_dense_rank,
        RANK() OVER (PARTITION BY plan ORDER BY plan_max_val DESC NULLS LAST) AS plan_max_val_rank,
        STDDEV_SAMP(plan_sum_val) OVER (PARTITION BY plan) AS plan_stddev_samp_plan_sum_val,
        SUM(plan_max_val) OVER (PARTITION BY plan) AS plan_sum_plan_max_val
    FROM kept_rows_price_tiers
)
SELECT
    id,
    plan,
    val,
    plan_count_val,
    plan_max_val,
    plan_sum_val,
    val_cume_dist,
    val_cume_dist_2,
    plan_sum_val_cume_dist
FROM with_windows_price_tiers_2
ORDER BY plan_sum_val_cume_dist DESC NULLS LAST
