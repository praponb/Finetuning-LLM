WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        EXPLODE(tier_limits) AS val
    FROM price_tiers
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        *,
        LOWER(plan) AS plan_lower,
        LAST_DAY(effective_from) AS effective_from_month_end
    FROM exploded
    WHERE val > 61
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        plan,
        effective_from,
        plan_lower,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val,
        PERCENT_RANK() OVER (ORDER BY val) AS val_percent_rank,
        MIN(val) OVER (PARTITION BY plan) AS plan_min_val,
        SUM(val) OVER (PARTITION BY plan) AS plan_sum_val
    FROM filtered
),
metrics_price_tiers AS (
    -- Window metrics over several different partitions
    SELECT
        plan,
        val_percent_rank,
        SUM(plan_sum_val) OVER (PARTITION BY plan) AS plan_sum_plan_sum_val
    FROM metrics
),
enriched AS (
    -- Ranking and gap features
    SELECT
        plan,
        val_percent_rank,
        RANK() OVER (PARTITION BY plan ORDER BY val_percent_rank DESC) AS val_percent_rank_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_sum_plan_sum_val DESC) AS plan_sum_plan_sum_val_cume_dist,
        DENSE_RANK() OVER w_running AS val_percent_rank_dense_rank
    FROM metrics_price_tiers
    WINDOW w_running AS (PARTITION BY plan ORDER BY val_percent_rank)
)
SELECT
    plan,
    val_percent_rank,
    val_percent_rank_rank
FROM enriched
ORDER BY val_percent_rank_rank DESC
