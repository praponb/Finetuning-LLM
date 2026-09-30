WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS val
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        MAX(val) OVER w_ordered AS plan_max_val,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val ASC) AS val_dense_rank,
        STDDEV_SAMP(val) OVER w_ordered AS plan_stddev_samp_val
    FROM lines
    WINDOW w_ordered AS (PARTITION BY plan)
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        STDDEV_SAMP(plan_stddev_samp_val) OVER w_part AS plan_stddev_samp_plan_stddev_samp_val,
        MIN(plan_stddev_samp_val) OVER w_part AS plan_min_plan_stddev_samp_val,
        RANK() OVER w_grp AS val_rank,
        PERCENT_RANK() OVER w_part_2 AS val_dense_rank_percent_rank,
        RANK() OVER (ORDER BY plan_stddev_samp_val DESC NULLS LAST) AS plan_stddev_samp_val_rank
    FROM scored
    WINDOW w_part AS (PARTITION BY plan), w_grp AS (PARTITION BY plan ORDER BY val DESC), w_part_2 AS (ORDER BY val_dense_rank ASC)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        val,
        plan_max_val,
        val_dense_rank,
        plan_stddev_samp_val,
        plan_min_plan_stddev_samp_val,
        val_rank,
        val_dense_rank_percent_rank,
        ROUND(plan_stddev_samp_val_rank, 1) AS plan_stddev_samp_val_rank_rounded
    FROM with_windows
    WHERE plan_stddev_samp_val_rank > 4
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER (ORDER BY val_rank ASC) AS val_rank_cume_dist,
        RANK() OVER (ORDER BY val_dense_rank_percent_rank DESC NULLS FIRST) AS val_dense_rank_percent_rank_rank
    FROM valid
)
SELECT
    val,
    plan_max_val,
    val_dense_rank,
    plan_min_plan_stddev_samp_val,
    val_rank,
    val_dense_rank_percent_rank,
    val_rank_cume_dist,
    val_dense_rank_percent_rank_rank
FROM enriched;
