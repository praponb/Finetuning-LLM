WITH lines AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) x AS val
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        plan,
        currency,
        MAX(val) OVER (PARTITION BY currency) AS currency_max_val,
        CUME_DIST() OVER (ORDER BY val DESC NULLS LAST) AS val_cume_dist
    FROM lines
    WHERE val < 71
),
cleaned AS (
    -- Basic cleansing and defaults
    SELECT *, SUBSTRING(plan, 1, 4) AS plan_prefix
    FROM scored
    WHERE plan = 'basic'
),
scored_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        plan,
        currency_max_val,
        plan_prefix,
        DENSE_RANK() OVER (PARTITION BY plan ORDER BY currency_max_val) AS currency_max_val_dense_rank,
        STDDEV_SAMP(val_cume_dist) OVER (PARTITION BY currency) AS currency_stddev_samp_val_cume_dist
    FROM cleaned
),
enriched AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        DENSE_RANK() OVER (ORDER BY currency_max_val_dense_rank NULLS LAST) AS currency_max_val_dense_rank_dense_rank,
        STDDEV_SAMP(currency_stddev_samp_val_cume_dist) OVER (PARTITION BY plan) AS plan_stddev_samp_currency_stddev_samp_val_cume_dist
    FROM scored_price_tiers
),
with_windows AS (
    -- Running and moving aggregates
    SELECT
        currency_max_val,
        currency_max_val_dense_rank,
        currency_stddev_samp_val_cume_dist,
        plan_stddev_samp_currency_stddev_samp_val_cume_dist,
        PERCENT_RANK() OVER (ORDER BY currency_stddev_samp_val_cume_dist DESC NULLS LAST) AS currency_stddev_samp_val_cume_dist_percent_rank,
        SUM(currency_max_val) OVER (PARTITION BY plan) AS plan_sum_currency_max_val
    FROM enriched
)
-- Grouped metrics
SELECT
    CASE WHEN plan_sum_currency_max_val >= 341 THEN 'LARGE' ELSE 'SMALL' END AS plan_sum_currency_max_val_tier,
    CASE WHEN currency_stddev_samp_val_cume_dist >= 1 THEN 'LARGE' ELSE 'SMALL' END AS currency_stddev_samp_val_cume_dist_tier,
    ROUND(AVG(currency_stddev_samp_val_cume_dist), 2) AS avg_currency_stddev_samp_val_cume_dist,
    COUNT(*) AS num_rows,
    APPROX_PERCENTILE(currency_max_val_dense_rank, ARRAY(0.25, 0.5, 0.75)) AS currency_max_val_dense_rank_quartiles,
    ROUND(AVG(currency_max_val_dense_rank), 2) AS avg_currency_max_val_dense_rank
FROM with_windows
GROUP BY ROLLUP (CASE WHEN plan_sum_currency_max_val >= 341 THEN 'LARGE' ELSE 'SMALL' END, CASE WHEN currency_stddev_samp_val_cume_dist >= 1 THEN 'LARGE' ELSE 'SMALL' END)
