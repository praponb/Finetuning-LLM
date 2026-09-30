WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val DESC NULLS LAST) val_percent_rank,
        DENSE_RANK() OVER (PARTITION BY currency ORDER BY val NULLS LAST) val_dense_rank
    FROM flattened
),
valid AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        plan,
        val,
        val_dense_rank,
        val * val_dense_rank val_times_val_dense_rank,
        COALESCE(val, 0) val_filled
    FROM enriched
    WHERE currency = 'USD'
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        *,
        RANK() OVER (ORDER BY val_dense_rank DESC NULLS LAST) val_dense_rank_rank,
        COUNT(val) OVER (PARTITION BY plan) plan_count_val,
        ROUND(val_dense_rank / SUM(val_dense_rank) OVER (PARTITION BY plan), 4) val_dense_rank_share,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY val_dense_rank) val_dense_rank_dense_rank
    FROM valid
),
with_windows_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        ROUND(plan_count_val / SUM(plan_count_val) OVER (PARTITION BY plan), 4) plan_count_val_share,
        STDDEV_SAMP(val_filled) OVER (PARTITION BY plan) plan_stddev_samp_val_filled,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY val_dense_rank_dense_rank DESC NULLS FIRST) val_dense_rank_dense_rank_cume_dist,
        CUME_DIST() OVER w_recent val_filled_cume_dist
    FROM with_windows
    WINDOW w_recent AS (PARTITION BY plan ORDER BY val_filled DESC NULLS LAST)
),
with_windows_price_tiers_2 AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(plan_stddev_samp_val_filled) OVER (PARTITION BY plan) plan_max_plan_stddev_samp_val_filled,
        SUM(plan_stddev_samp_val_filled) OVER (PARTITION BY plan) plan_sum_plan_stddev_samp_val_filled
    FROM with_windows_price_tiers
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY plan_stddev_samp_val_filled DESC) plan_stddev_samp_val_filled_percent_rank,
        ROUND(val_dense_rank / SUM(val_dense_rank) OVER (PARTITION BY plan), 4) val_dense_rank_share_2,
        RANK() OVER (ORDER BY val NULLS FIRST) val_rank
    FROM with_windows_price_tiers_2
)
-- Window metrics over several different partitions
SELECT
    *,
    RANK() OVER w_recent val_dense_rank_dense_rank_rank,
    RANK() OVER (PARTITION BY plan ORDER BY val_dense_rank NULLS LAST) val_dense_rank_rank_2,
    SUM(val_filled) OVER w_grp plan_sum_val_filled
FROM scored
WINDOW w_recent AS (ORDER BY val_dense_rank_dense_rank DESC NULLS FIRST), w_grp AS (PARTITION BY plan);
