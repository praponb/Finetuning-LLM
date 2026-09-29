WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW EXPLODE(tier_limits) t AS val
),
cleaned AS (
    -- Keep recent, valid rows
    SELECT
        id,
        plan,
        effective_from,
        val,
        ROUND(val, 2) AS val_rounded
    FROM lines
    WHERE effective_from > TO_DATE('2024-06-07')
),
remaining_keys AS (
    -- Set intersection on keys
    SELECT id
    FROM cleaned
    INTERSECT ALL
    SELECT id
    FROM price_tiers
    WHERE effective_from <= '2025-08-30'
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM cleaned s
    JOIN remaining_keys k ON s.id = k.id
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        MAX(val_rounded) OVER (PARTITION BY plan) AS plan_max_val_rounded,
        AVG(val_rounded) OVER (PARTITION BY plan) AS plan_avg_val_rounded,
        STDDEV_SAMP(val) OVER (PARTITION BY plan) AS plan_stddev_samp_val,
        STDDEV_SAMP(val_rounded) OVER (PARTITION BY plan) AS plan_stddev_samp_val_rounded
    FROM kept_rows
),
daily AS (
    -- Aggregate with conditional metrics
    SELECT
        DATE_TRUNC('MONTH', effective_from) AS activity_month,
        DATE_TRUNC('WEEK', effective_from) AS week_start,
        CASE WHEN plan_avg_val_rounded >= 81 THEN 'LARGE' ELSE 'SMALL' END AS plan_avg_val_rounded_tier,
        SUM(CASE WHEN plan = 'enterprise' THEN val ELSE 0 END) AS enterprise_val,
        SUM(plan_stddev_samp_val) FILTER (WHERE plan = 'starter') AS starter_total_plan_stddev_samp_val
    FROM metrics
    GROUP BY 1, 2, 3
)
SELECT
    activity_month,
    week_start,
    plan_avg_val_rounded_tier,
    enterprise_val,
    starter_total_plan_stddev_samp_val
FROM daily;
