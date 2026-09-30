WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(discounts) li AS val
),
scored AS (
    -- Running and moving aggregates
    SELECT
        *,
        SUM(val) OVER w_hist AS plan_sum_val,
        SUM(val) OVER w_ordered AS currency_sum_val,
        SUM(val) OVER w_ordered AS currency_sum_val_2
    FROM lines
    WHERE val <= 33
    WINDOW w_hist AS (PARTITION BY plan), w_ordered AS (PARTITION BY currency)
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        RANK() OVER w_recent AS currency_sum_val_rank,
        SUM(currency_sum_val) OVER (PARTITION BY plan, currency) AS plan_sum_currency_sum_val,
        MAX(plan_sum_val) OVER w_recent_2 AS plan_max_plan_sum_val,
        STDDEV_SAMP(val) OVER w_recent_2 AS plan_stddev_samp_val
    FROM scored
    WINDOW w_recent AS (ORDER BY currency_sum_val DESC NULLS LAST), w_recent_2 AS (PARTITION BY plan)
),
valid AS (
    -- Keep recent, valid rows
    SELECT *, plan_stddev_samp_val * currency_sum_val_rank AS plan_stddev_samp_val_times_currency_sum_val_rank
    FROM enriched
    WHERE currency_sum_val_rank < 7
),
enriched_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        currency,
        val,
        plan_sum_val,
        plan_max_plan_sum_val,
        plan_stddev_samp_val,
        plan_stddev_samp_val_times_currency_sum_val_rank,
        COUNT(val) OVER (PARTITION BY plan, currency) AS plan_count_val
    FROM valid
),
stats AS (
    -- Multi-level aggregation
    SELECT
        CASE WHEN plan_sum_val >= 338 THEN 'LARGE' ELSE 'SMALL' END AS plan_sum_val_tier,
        COUNT(CASE WHEN currency = 'USD' THEN 1 END) AS usd_count,
        ROUND(SUM(val) / NULLIF(COUNT(CASE WHEN currency = 'USD' THEN 1 END), 0), 2) AS val_per_usd,
        PERCENTILE_APPROX(plan_sum_val, 0.5) AS plan_sum_val_p50,
        AVG(plan_sum_val) FILTER (WHERE plan = 'pro') AS pro_avg_plan_sum_val
    FROM enriched_price_tiers
    GROUP BY plan_sum_val_tier
)
SELECT
    plan_sum_val_tier,
    usd_count,
    val_per_usd,
    plan_sum_val_p50,
    pro_avg_plan_sum_val
FROM stats;
