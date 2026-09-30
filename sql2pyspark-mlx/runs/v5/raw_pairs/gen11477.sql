WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE_OUTER(tier_prices) AS val
    FROM price_tiers
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        id,
        effective_from,
        currency,
        val,
        COUNT(val) OVER w_part AS plan_count_val,
        MAX(val) OVER w_part AS plan_max_val,
        COUNT(val) OVER w_part AS plan_count_val_2,
        SUM(val) OVER (PARTITION BY currency) AS currency_sum_val,
        PERCENT_RANK() OVER w_hist AS val_percent_rank
    FROM flattened
    WINDOW w_part AS (PARTITION BY plan, currency), w_hist AS (PARTITION BY currency ORDER BY val DESC NULLS FIRST)
),
valid AS (
    -- Filter the raw rows and derive helper columns
    SELECT *, CASE WHEN val >= 61 THEN 'High' WHEN val >= 25 THEN 'Medium' ELSE 'Low' END AS val_band
    FROM enriched
    WHERE effective_from >= DATE '2024-02-17'
      AND val < 75
),
with_windows AS (
    -- Window metrics over several different partitions
    SELECT
        effective_from,
        currency,
        val,
        plan_count_val,
        plan_count_val_2,
        val_percent_rank,
        val_band,
        AVG(val_percent_rank) OVER (PARTITION BY currency) AS currency_avg_val_percent_rank,
        MIN(plan_max_val) OVER (PARTITION BY currency) AS currency_min_plan_max_val,
        CUME_DIST() OVER (PARTITION BY currency ORDER BY plan_count_val) AS plan_count_val_cume_dist
    FROM valid
),
metrics AS (
    -- Window metrics over several different partitions
    SELECT
        effective_from,
        currency,
        val,
        plan_count_val,
        val_percent_rank,
        plan_count_val_cume_dist,
        SUM(val_percent_rank) OVER w_hist AS currency_sum_val_percent_rank
    FROM with_windows
    WINDOW w_hist AS (PARTITION BY currency)
)
SELECT
    effective_from,
    currency,
    val,
    plan_count_val,
    val_percent_rank,
    plan_count_val_cume_dist,
    currency_sum_val_percent_rank,
    CASE WHEN currency RLIKE '^(THB|USD)$' THEN 'GROUP_A' ELSE 'CORE' END AS currency_zone
FROM metrics;
