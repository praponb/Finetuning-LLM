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
kept_keys AS (
    -- Set difference on keys
    SELECT id, plan
    FROM lines
    EXCEPT DISTINCT
    SELECT id, plan
    FROM price_tiers
    WHERE MONTH(effective_from) IN (2, 3, 4)
),
kept_rows AS (
    -- Keep rows whose keys survive the set operation
    SELECT s.*
    FROM lines s
    JOIN kept_keys k ON s.id = k.id AND s.plan = k.plan
),
scored AS (
    -- Ranking and gap features
    SELECT
        *,
        AVG(val) OVER (PARTITION BY plan) AS plan_avg_val,
        STDDEV_SAMP(val) OVER (PARTITION BY currency) AS currency_stddev_samp_val,
        SUM(val) OVER (PARTITION BY currency) AS currency_sum_val,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val,
        MAX(val) OVER (PARTITION BY currency, plan) AS currency_max_val
    FROM kept_rows
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        plan_avg_val,
        currency_stddev_samp_val,
        currency_sum_val,
        plan_count_val,
        currency_max_val,
        RANK() OVER (PARTITION BY currency ORDER BY val NULLS FIRST) AS val_rank,
        CUME_DIST() OVER (ORDER BY plan_count_val DESC) AS plan_count_val_cume_dist
    FROM scored
),
stats AS (
    -- Grouped metrics
    SELECT
        CASE WHEN plan_count_val >= 17 THEN 'LARGE' ELSE 'SMALL' END AS plan_count_val_tier,
        MAX(val) FILTER (WHERE currency = 'USD') AS usd_max_val,
        COUNT(DISTINCT currency) AS distinct_currencies,
        COUNT_IF(plan = 'starter') AS starter_events,
        COUNT(*) AS row_count,
        SUM(plan_count_val_cume_dist) AS total_plan_count_val_cume_dist
    FROM scored_price_tiers
    GROUP BY 1
    HAVING usd_max_val >= 0
)
SELECT
    plan_count_val_tier,
    usd_max_val,
    row_count,
    total_plan_count_val_cume_dist
FROM stats;
