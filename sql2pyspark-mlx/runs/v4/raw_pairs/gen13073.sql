WITH flattened AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        effective_from,
        currency,
        EXPLODE(discounts) AS val
    FROM price_tiers
),
metrics AS (
    -- Ranking and gap features
    SELECT
        *,
        PERCENT_RANK() OVER (PARTITION BY plan ORDER BY val) AS val_percent_rank,
        AVG(val) OVER (PARTITION BY plan) AS plan_avg_val,
        MAX(val) OVER w_running AS plan_max_val
    FROM flattened
    WINDOW w_running AS (PARTITION BY plan, currency)
),
enriched AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        val_percent_rank,
        plan_avg_val,
        plan_max_val,
        CUME_DIST() OVER (ORDER BY val_percent_rank) AS val_percent_rank_cume_dist,
        COUNT(val) OVER (PARTITION BY plan) AS plan_count_val,
        STDDEV_SAMP(val) OVER (PARTITION BY currency, plan) AS currency_stddev_samp_val
    FROM metrics
),
enriched_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        plan,
        effective_from,
        currency,
        val,
        val_percent_rank,
        plan_avg_val,
        plan_max_val,
        val_percent_rank_cume_dist,
        plan_count_val,
        currency_stddev_samp_val,
        DENSE_RANK() OVER (ORDER BY currency_stddev_samp_val NULLS LAST) AS currency_stddev_samp_val_dense_rank,
        CUME_DIST() OVER (PARTITION BY currency, plan ORDER BY val DESC) AS val_cume_dist
    FROM enriched
)
-- Aggregate with conditional metrics
SELECT
    plan,
    DATE_TRUNC('YEAR', effective_from) AS year_start,
    APPROX_PERCENTILE(currency_stddev_samp_val_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS currency_stddev_samp_val_dense_rank_quartiles,
    COUNT(*) AS row_count,
    COUNT(CASE WHEN plan = 'starter' THEN 1 END) AS starter_count
FROM enriched_price_tiers
GROUP BY plan, DATE_TRUNC('YEAR', effective_from)
ORDER BY starter_count DESC NULLS LAST;
