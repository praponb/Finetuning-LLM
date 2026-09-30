WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(tier_prices) AS val
    FROM price_tiers
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        val,
        PERCENT_RANK() OVER (ORDER BY val) AS val_percent_rank,
        CUME_DIST() OVER (ORDER BY val NULLS FIRST) AS val_cume_dist
    FROM flattened
),
enriched AS (
    -- Ranking and gap features
    SELECT
        plan,
        val_percent_rank,
        val_cume_dist,
        STDDEV_SAMP(val) OVER w_ordered AS plan_stddev_samp_val,
        STDDEV_SAMP(val_percent_rank) OVER w_ordered AS plan_stddev_samp_val_percent_rank,
        COUNT(val_percent_rank) OVER w_ordered AS plan_count_val_percent_rank,
        DENSE_RANK() OVER (ORDER BY val_percent_rank DESC) AS val_percent_rank_dense_rank
    FROM metrics
    WINDOW w_ordered AS (PARTITION BY plan)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, ROUND(val_cume_dist / plan_count_val_percent_rank, 2) AS val_cume_dist_per_plan_count_val_percent_rank
    FROM enriched
    WHERE plan IN ('basic', 'enterprise', 'starter')
),
filtered AS (
    -- Keep recent, valid rows
    SELECT *, ROUND(plan_stddev_samp_val_percent_rank, 0) AS plan_stddev_samp_val_percent_rank_rounded
    FROM cleaned
    WHERE val_percent_rank_dense_rank = 5
),
metrics_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        *,
        CUME_DIST() OVER w_running AS val_percent_rank_cume_dist,
        AVG(val_percent_rank) OVER w_running_2 AS plan_avg_val_percent_rank,
        SUM(val_percent_rank_dense_rank) OVER w_running_2 AS plan_sum_val_percent_rank_dense_rank,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val_percent_rank DESC) AS val_percent_rank_percent_rank
    FROM filtered
    WINDOW w_running AS (ORDER BY val_percent_rank DESC NULLS FIRST), w_running_2 AS (PARTITION BY plan)
),
report AS (
    SELECT
        val_percent_rank,
        val_cume_dist,
        plan_stddev_samp_val,
        plan_stddev_samp_val_percent_rank,
        plan_count_val_percent_rank,
        plan_stddev_samp_val_percent_rank_rounded,
        plan_avg_val_percent_rank,
        val_percent_rank_percent_rank
    FROM metrics_price_tiers
)
SELECT
    val_cume_dist,
    plan_stddev_samp_val_percent_rank_rounded,
    val_percent_rank_percent_rank
FROM report
ORDER BY val_cume_dist DESC NULLS LAST, val_percent_rank_percent_rank
