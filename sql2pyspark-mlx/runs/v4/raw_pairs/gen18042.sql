WITH exploded AS (
    -- Unnest the array into one row per element
    SELECT
        id,
        plan,
        currency,
        val
    FROM price_tiers
    LATERAL VIEW OUTER EXPLODE(tier_prices) e AS val
),
enriched AS (
    -- Ranking and gap features
    SELECT
        *,
        COUNT(val) OVER (PARTITION BY plan, currency) AS plan_count_val,
        PERCENT_RANK() OVER w_recent AS val_percent_rank,
        MAX(val) OVER w_hist AS plan_max_val
    FROM exploded
    WINDOW w_recent AS (PARTITION BY plan ORDER BY val ASC), w_hist AS (PARTITION BY plan)
),
cleaned AS (
    -- Filter the raw rows and derive helper columns
    SELECT
        *,
        plan_count_val * val_percent_rank AS plan_count_val_times_val_percent_rank,
        UPPER(plan) AS plan_upper
    FROM enriched
    WHERE currency IN ('THB', 'EUR')
),
metrics AS (
    -- Running and moving aggregates
    SELECT
        *,
        STDDEV_SAMP(val_percent_rank) OVER (PARTITION BY currency) AS currency_stddev_samp_val_percent_rank,
        CUME_DIST() OVER (PARTITION BY plan ORDER BY plan_count_val DESC NULLS LAST) AS plan_count_val_cume_dist
    FROM cleaned
),
filtered AS (
    -- Keep recent, valid rows
    SELECT
        id,
        val,
        plan_upper,
        CASE WHEN currency_stddev_samp_val_percent_rank >= 1 THEN 'High' WHEN currency_stddev_samp_val_percent_rank >= 0 THEN 'Medium' ELSE 'Low' END AS currency_stddev_samp_val_percent_rank_band,
        SUBSTRING(currency, 1, 2) AS currency_prefix
    FROM metrics
    WHERE plan_count_val_times_val_percent_rank BETWEEN 44 AND 57
),
metrics_price_tiers AS (
    -- Running and moving aggregates
    SELECT
        id,
        val,
        plan_upper,
        currency_stddev_samp_val_percent_rank_band,
        currency_prefix,
        DENSE_RANK() OVER w_grp AS val_dense_rank,
        DENSE_RANK() OVER w_grp AS val_dense_rank_2,
        CUME_DIST() OVER (ORDER BY val ASC NULLS LAST) AS val_cume_dist
    FROM filtered
    WINDOW w_grp AS (ORDER BY val ASC)
),
scored AS (
    -- Window metrics over several different partitions
    SELECT
        *,
        PERCENT_RANK() OVER (ORDER BY val_dense_rank ASC) AS val_dense_rank_percent_rank,
        CUME_DIST() OVER (ORDER BY val_dense_rank DESC) AS val_dense_rank_cume_dist,
        PERCENT_RANK() OVER (ORDER BY val_cume_dist DESC NULLS LAST) AS val_cume_dist_percent_rank,
        RANK() OVER (ORDER BY val_dense_rank ASC NULLS FIRST) AS val_dense_rank_rank,
        PERCENT_RANK() OVER (ORDER BY val DESC NULLS LAST) AS val_percent_rank
    FROM metrics_price_tiers
)
SELECT
    val,
    plan_upper,
    currency_prefix,
    val_cume_dist,
    val_dense_rank_percent_rank,
    val_dense_rank_cume_dist,
    val_cume_dist_percent_rank,
    val_percent_rank
FROM scored
