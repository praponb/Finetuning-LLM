WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS val
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        MIN(val) OVER w_recent AS plan_min_val,
        RANK() OVER (ORDER BY val NULLS FIRST) AS val_rank,
        COUNT(val) OVER w_recent AS plan_count_val,
        RANK() OVER w_running AS val_rank_2,
        STDDEV_SAMP(val) OVER w_recent AS plan_stddev_samp_val
    FROM flattened
    WHERE val BETWEEN 46 AND 55
    WINDOW w_recent AS (PARTITION BY plan), w_running AS (ORDER BY val DESC)
),
base AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, plan_stddev_samp_val + plan_min_val AS plan_stddev_samp_val_plus_plan_min_val
    FROM metrics
    WHERE effective_from >= DATE '2024-04-25'
      AND val_rank = 6
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT
        id,
        val_rank,
        plan_count_val,
        val_rank_2,
        plan_stddev_samp_val,
        plan_stddev_samp_val_plus_plan_min_val,
        CAST(plan_min_val AS STRING) AS plan_min_val_str
    FROM base
    WHERE effective_from >= TO_DATE('2024-01-31')
),
base_price_tiers AS (
    -- Basic cleansing and defaults
    SELECT
        *,
        ROUND(plan_stddev_samp_val_plus_plan_min_val, 0) AS plan_stddev_samp_val_plus_plan_min_val_rounded,
        ROUND(val_rank_2 / val_rank, 2) AS val_rank_2_per_val_rank
    FROM cleaned
    WHERE plan_stddev_samp_val >= 52
)
SELECT
    id,
    val_rank,
    plan_stddev_samp_val,
    plan_stddev_samp_val_plus_plan_min_val,
    plan_min_val_str,
    val_rank_2_per_val_rank
FROM base_price_tiers
ORDER BY val_rank NULLS LAST;
