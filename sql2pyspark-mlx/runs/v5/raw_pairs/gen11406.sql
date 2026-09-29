WITH flattened AS (
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) x AS val
),
metrics AS (
    SELECT
        *,
        MAX(val) OVER (PARTITION BY currency) AS currency_max_val,
        SUM(val) OVER w_running AS currency_sum_val,
        MIN(val) OVER w_recent AS plan_min_val
    FROM flattened
    WHERE val IS NOT NULL
    WINDOW w_running AS (PARTITION BY currency, plan), w_recent AS (PARTITION BY plan)
),
filtered AS (
    SELECT
        id,
        plan,
        currency,
        val,
        currency_max_val,
        currency_sum_val,
        plan_min_val,
        LENGTH(currency) AS currency_length,
        UPPER(currency) AS currency_upper
    FROM metrics
    WHERE plan = 'basic'
),
valid AS (
    SELECT
        *,
        TRIM(plan) AS clean_plan,
        val + plan_min_val AS val_plus_plan_min_val
    FROM filtered
    WHERE plan_min_val >= 67
),
with_windows AS (
    SELECT
        plan,
        currency_max_val,
        currency_sum_val,
        currency_length,
        clean_plan,
        val_plus_plan_min_val,
        COUNT(currency_sum_val) OVER (PARTITION BY plan) AS plan_count_currency_sum_val,
        STDDEV_SAMP(val) OVER w_part AS currency_stddev_samp_val,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_min_val DESC NULLS LAST) AS plan_min_val_cume_dist,
        MAX(val) OVER (PARTITION BY currency) AS currency_max_val_2,
        DENSE_RANK() OVER (PARTITION BY currency, plan ORDER BY currency_max_val ASC NULLS FIRST) AS currency_max_val_dense_rank
    FROM valid
    WINDOW w_part AS (PARTITION BY currency, plan)
),
daily AS (
    SELECT
        CASE WHEN currency_max_val >= 61 THEN 'HIGH' ELSE 'LOW' END AS currency_max_val_tier,
        CASE WHEN plan_count_currency_sum_val >= 14 THEN 'HIGH' ELSE 'LOW' END AS plan_count_currency_sum_val_tier,
        plan,
        COUNT(*) AS event_count,
        COUNT_IF(plan = 'pro') AS pro_events,
        SIZE(COLLECT_SET(plan)) AS num_plans
    FROM with_windows
    GROUP BY 1, 2, 3
    HAVING COUNT_IF(plan = 'pro') >= 1
)
SELECT
    currency_max_val_tier,
    plan_count_currency_sum_val_tier,
    plan,
    pro_events,
    num_plans
FROM daily
ORDER BY num_plans DESC NULLS LAST;
